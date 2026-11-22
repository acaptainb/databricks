// Parquet writers for the two output tables.
//
// A minimal writer: one row group per file, one format-v1 data page per
// column, PLAIN encoding, no compression, no statistics. The footer is Thrift
// compact protocol written by hand. Each file is built in memory and written
// with one call.

use std::path::Path;

use crate::UserRow;

type S = &'static [u8];

pub struct CampRow {
    pub id: S,
    pub channel: S,
    pub day: i32,
    pub sessions: u64,
    pub purchasing: u64,
    pub revenue: u64,
    pub assisted: u64,
}

// Parquet enums
const T_INT32: i32 = 1;
const T_INT64: i32 = 2;
const T_BYTE_ARRAY: i32 = 6;
const REQUIRED: i32 = 0;
const OPTIONAL: i32 = 1;
const CT_UTF8: i32 = 0;
const CT_DATE: i32 = 6;
const ENC_PLAIN: i32 = 0;
const ENC_RLE: i32 = 3;
const PAGE_DATA: i32 = 0;
const CODEC_NONE: i32 = 0;

// Thrift compact types
const TC_I32: u8 = 5;
const TC_I64: u8 = 6;
const TC_BINARY: u8 = 8;
const TC_LIST: u8 = 9;
const TC_STRUCT: u8 = 12;

/// Thrift compact-protocol writer.
struct Tw {
    b: Vec<u8>,
    last: Vec<i16>, // last field id per open struct
}

impl Tw {
    fn new() -> Tw {
        Tw { b: Vec::with_capacity(1024), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }
    fn zigzag(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = self.last.last_mut().unwrap();
        let d = id - *last;
        if (1..=15).contains(&d) {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            let v = id as i64;
            self.varint(((v << 1) ^ (v >> 63)) as u64);
        }
        *last = id;
    }
    fn i32f(&mut self, id: i16, v: i32) {
        self.field(id, TC_I32);
        self.zigzag(v as i64);
    }
    fn i64f(&mut self, id: i16, v: i64) {
        self.field(id, TC_I64);
        self.zigzag(v);
    }
    fn binf(&mut self, id: i16, s: &[u8]) {
        self.field(id, TC_BINARY);
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s);
    }
    fn list_header(&mut self, n: usize, elem: u8) {
        if n < 15 {
            self.b.push(((n as u8) << 4) | elem);
        } else {
            self.b.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
    fn listf(&mut self, id: i16, n: usize, elem: u8) {
        self.field(id, TC_LIST);
        self.list_header(n, elem);
    }
    /// Begin a struct that is a field value (after `structf`) or a list element.
    fn begin(&mut self) {
        self.last.push(0);
    }
    fn structf(&mut self, id: i16) {
        self.field(id, TC_STRUCT);
        self.begin();
    }
    fn end(&mut self) {
        self.b.push(0);
        self.last.pop();
    }
}

/// One column: its schema and its encoded page body (levels + PLAIN values).
struct Col {
    name: &'static str,
    ty: i32,
    rep: i32,
    conv: Option<i32>,
    body: Vec<u8>,
}

fn col_i64(name: &'static str, vals: impl Iterator<Item = i64>, n: usize) -> Col {
    let mut body = Vec::with_capacity(n * 8);
    for v in vals {
        body.extend_from_slice(&v.to_le_bytes());
    }
    Col { name, ty: T_INT64, rep: REQUIRED, conv: None, body }
}

fn col_date(name: &'static str, vals: impl Iterator<Item = i32>, n: usize) -> Col {
    let mut body = Vec::with_capacity(n * 4);
    for v in vals {
        body.extend_from_slice(&v.to_le_bytes());
    }
    Col { name, ty: T_INT32, rep: REQUIRED, conv: Some(CT_DATE), body }
}

fn col_str<'a>(name: &'static str, vals: impl Iterator<Item = &'a [u8]>, cap: usize) -> Col {
    let mut body = Vec::with_capacity(cap);
    for s in vals {
        body.extend_from_slice(&(s.len() as u32).to_le_bytes());
        body.extend_from_slice(s);
    }
    Col { name, ty: T_BYTE_ARRAY, rep: REQUIRED, conv: Some(CT_UTF8), body }
}

/// An optional string column: v1 definition levels (bit width 1, one
/// bit-packed run, 4-byte length prefix), then PLAIN values of the non-null rows.
fn col_opt_str<'a>(name: &'static str, vals: impl Iterator<Item = Option<&'a [u8]>>, n: usize) -> Col {
    if n == 0 {
        // no values: empty level data
        return Col { name, ty: T_BYTE_ARRAY, rep: OPTIONAL, conv: Some(CT_UTF8), body: 0u32.to_le_bytes().to_vec() };
    }
    let groups = n.div_ceil(8);
    let mut levels = vec![0u8; groups];
    let mut values: Vec<u8> = Vec::with_capacity(n * 8);
    for (i, v) in vals.enumerate() {
        if let Some(s) = v {
            levels[i / 8] |= 1 << (i % 8);
            values.extend_from_slice(&(s.len() as u32).to_le_bytes());
            values.extend_from_slice(s);
        }
    }
    let mut hdr = Tw::new();
    hdr.varint(((groups as u64) << 1) | 1);
    let mut body = Vec::with_capacity(4 + hdr.b.len() + groups + values.len());
    body.extend_from_slice(&((hdr.b.len() + groups) as u32).to_le_bytes());
    body.extend_from_slice(&hdr.b);
    body.extend_from_slice(&levels);
    body.extend_from_slice(&values);
    Col { name, ty: T_BYTE_ARRAY, rep: OPTIONAL, conv: Some(CT_UTF8), body }
}

fn write_file(path: &Path, cols: Vec<Col>, nrows: usize) {
    let total: usize = cols.iter().map(|c| c.body.len() + 64).sum::<usize>() + 4096;
    let mut out: Vec<u8> = Vec::with_capacity(total);
    out.extend_from_slice(b"PAR1");
    // (data_page_offset, chunk bytes) per column
    let mut chunks: Vec<(i64, i64)> = Vec::with_capacity(cols.len());
    for c in &cols {
        let mut h = Tw::new();
        h.i32f(1, PAGE_DATA);
        h.i32f(2, c.body.len() as i32);
        h.i32f(3, c.body.len() as i32);
        h.structf(5);
        h.i32f(1, nrows as i32);
        h.i32f(2, ENC_PLAIN);
        h.i32f(3, ENC_RLE);
        h.i32f(4, ENC_RLE);
        h.end();
        h.b.push(0); // end PageHeader
        let off = out.len() as i64;
        out.extend_from_slice(&h.b);
        out.extend_from_slice(&c.body);
        chunks.push((off, (h.b.len() + c.body.len()) as i64));
    }
    let row_group_bytes: i64 = chunks.iter().map(|c| c.1).sum();

    let mut f = Tw::new();
    f.i32f(1, 1); // version
    f.listf(2, cols.len() + 1, TC_STRUCT);
    f.begin();
    f.binf(4, b"schema");
    f.i32f(5, cols.len() as i32);
    f.end();
    for c in &cols {
        f.begin();
        f.i32f(1, c.ty);
        f.i32f(3, c.rep);
        f.binf(4, c.name.as_bytes());
        if let Some(ct) = c.conv {
            f.i32f(6, ct);
        }
        f.end();
    }
    f.i64f(3, nrows as i64);
    f.listf(4, 1, TC_STRUCT);
    f.begin(); // RowGroup
    f.listf(1, cols.len(), TC_STRUCT);
    for (c, &(off, len)) in cols.iter().zip(&chunks) {
        f.begin(); // ColumnChunk
        f.i64f(2, off);
        f.structf(3); // ColumnMetaData
        f.i32f(1, c.ty);
        f.listf(2, 2, TC_I32);
        f.zigzag(ENC_PLAIN as i64);
        f.zigzag(ENC_RLE as i64);
        f.listf(3, 1, TC_BINARY);
        f.varint(c.name.len() as u64);
        f.b.extend_from_slice(c.name.as_bytes());
        f.i32f(4, CODEC_NONE);
        f.i64f(5, nrows as i64);
        f.i64f(6, len);
        f.i64f(7, len);
        f.i64f(9, off);
        f.end(); // ColumnMetaData
        f.end(); // ColumnChunk
    }
    f.i64f(2, row_group_bytes);
    f.i64f(3, nrows as i64);
    f.end(); // RowGroup
    f.binf(6, b"shopping-sessions");
    f.b.push(0); // end FileMetaData

    out.extend_from_slice(&f.b);
    out.extend_from_slice(&(f.b.len() as u32).to_le_bytes());
    out.extend_from_slice(b"PAR1");
    std::fs::write(path, &out).expect("write parquet");
}

pub fn write_user(path: &Path, rows: &[UserRow], cats: &[S]) {
    let n = rows.len();
    let name_bytes: usize = rows.iter().map(|r| r.name.as_bytes().len() + 4).sum();
    let cols = vec![
        col_str("user_id", rows.iter().map(|r| r.name.as_bytes()), name_bytes),
        col_date("day", rows.iter().map(|r| r.day), n),
        col_i64("sessions", rows.iter().map(|r| r.sessions as i64), n),
        col_i64("events", rows.iter().map(|r| r.events as i64), n),
        col_i64("revenue_cents", rows.iter().map(|r| r.revenue as i64), n),
        col_opt_str("top_category", rows.iter().map(|r| if r.top >= 0 { Some(cats[r.top as usize]) } else { None }), n),
    ];
    write_file(path, cols, n);
}

pub fn write_campaign(path: &Path, rows: &[CampRow]) {
    let n = rows.len();
    let cap = |f: fn(&CampRow) -> S| rows.iter().map(|r| f(r).len() + 4).sum::<usize>();
    let cols = vec![
        col_str("campaign_id", rows.iter().map(|r| r.id), cap(|r| r.id)),
        col_str("channel", rows.iter().map(|r| r.channel), cap(|r| r.channel)),
        col_date("day", rows.iter().map(|r| r.day), n),
        col_i64("sessions", rows.iter().map(|r| r.sessions as i64), n),
        col_i64("purchasing_sessions", rows.iter().map(|r| r.purchasing as i64), n),
        col_i64("revenue_cents", rows.iter().map(|r| r.revenue as i64), n),
        col_i64("assisted_sessions", rows.iter().map(|r| r.assisted as i64), n),
    ];
    write_file(path, cols, n);
}
