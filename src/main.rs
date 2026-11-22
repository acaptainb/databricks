// Shopping Sessions pipeline in Rust.
// Reads $DATA_DIR, writes $OUTPUT_DIR/user_daily and $OUTPUT_DIR/campaign_daily as Parquet.
//
// Phases (all parallel):
//  1. parse events (bucketed by event_id hash), clicks (by user hash) and links
//     (union-find, partitioned by ID hash), all at once
//  2. dedup each event bucket with a small hash table, emit survivors by user hash
//  3. per user-hash partition: map each user ID to its person, re-bucket by person hash
//  4. per person partition: sort, sessionize, attribute, aggregate
//  5. write Parquet

use hashbrown::HashTable;
use mimalloc::MiMalloc;
use rayon::prelude::*;
use std::path::{Path, PathBuf};
use std::time::Instant;

mod output;
mod parse;
mod small;

use parse::*;
use small::Small;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub type S = &'static [u8];

pub const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const LATE: i64 = DAY;
pub const MAX_PRICE: u64 = 10_000_000;

const DB_BITS: u32 = 10; // dedup buckets, by event_id hash (top bits)
const DB: usize = 1 << DB_BITS;
const PP_BITS: u32 = 8; // user / person partitions (bits 40..)
const PP: usize = 1 << PP_BITS;
pub const CHUNK: usize = 4 << 20;


static T0: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
static VERBOSE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Timestamped stage note on stderr when SS_VERBOSE is set.
fn vlog(m: &str) {
    if VERBOSE.load(std::sync::atomic::Ordering::Relaxed) {
        let t = T0.get().map(|t| t.elapsed().as_secs_f64()).unwrap_or(0.0);
        eprintln!("    [{t:7.3}s] {m}");
    }
}
#[inline(always)]
pub fn hb(b: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(b)
}

#[inline(always)]
fn upart(h: u64) -> usize {
    // bits clear of hashbrown's index (low) and tag (top 7) bits, and of the dedup bits
    ((h >> 40) as usize) & (PP - 1)
}

// ---------------------------------------------------------------- small lookup tables

/// String -> index map keyed by Small (no memcmp on lookup).
struct StrMap {
    table: HashTable<u32>,
    keys: Vec<S>,
    sk: Vec<Small>,
}

impl StrMap {
    fn new() -> Self {
        StrMap { table: HashTable::new(), keys: Vec::new(), sk: Vec::new() }
    }
    #[inline(always)]
    fn get(&self, k: Small) -> Option<u32> {
        self.table.find(k.h(), |&i| self.sk[i as usize] == k).copied()
    }
    fn intern(&mut self, k: S) -> u32 {
        let s = Small::new(k);
        let h = s.h();
        let sk = &self.sk;
        if let Some(&i) = self.table.find(h, |&i| sk[i as usize] == s) {
            return i;
        }
        let i = self.keys.len() as u32;
        self.keys.push(k);
        self.sk.push(s);
        let sk = &self.sk;
        self.table.insert_unique(h, i, |&j| sk[j as usize].h());
        i
    }
}

fn parse_simple2(data: S) -> Vec<[S; 2]> {
    let mut out = Vec::new();
    let Some(nl) = memchr::memchr(b'\n', data) else { return out };
    for_lines(&data[nl + 1..], |line| {
        if let Some(f) = split::<2>(line) {
            out.push(f);
        }
    });
    out
}

struct Products {
    map: StrMap,
    cat_of: Vec<u16>,
    unknown: u16,
    cats: Vec<S>,
}

fn load_products(data: S) -> Products {
    let rows = parse_simple2(data);
    let mut names: Vec<S> = rows.iter().map(|r| r[1]).collect();
    names.push(b"unknown");
    names.sort();
    names.dedup();
    let idx = |c: &[u8]| names.binary_search_by(|x| (*x).cmp(c)).unwrap() as u16;
    let mut map = StrMap::new();
    let mut cat_of = Vec::new();
    for r in &rows {
        let i = map.intern(r[0]);
        if i as usize == cat_of.len() {
            cat_of.push(idx(r[1]));
        }
    }
    let unknown = idx(b"unknown");
    Products { map, cat_of, unknown, cats: names }
}

struct Campaigns {
    map: StrMap,
    channel: Vec<S>,
}

fn load_campaigns(data: S) -> Campaigns {
    let rows = parse_simple2(data);
    let mut map = StrMap::new();
    let mut channel = Vec::new();
    for r in &rows {
        let i = map.intern(r[0]);
        if i as usize == channel.len() {
            channel.push(r[1]);
        }
    }
    Campaigns { map, channel }
}

// ---------------------------------------------------------------- linked IDs

/// One hash partition of the linked-ID -> person table.
pub struct IdPart {
    table: HashTable<u32>,
    keys: Vec<Small>,
    hashes: Vec<u64>,
    name: Vec<Small>,
}

impl IdPart {
    fn new() -> Self {
        IdPart { table: HashTable::new(), keys: Vec::new(), hashes: Vec::new(), name: Vec::new() }
    }
    #[inline(always)]
    fn intern(&mut self, k: Small, h: u64) -> u32 {
        let keys = &self.keys;
        if let Some(&i) = self.table.find(h, |&i| keys[i as usize] == k) {
            return i;
        }
        let i = self.keys.len() as u32;
        self.keys.push(k);
        self.hashes.push(h);
        let hs = &self.hashes;
        self.table.insert_unique(h, i, |&j| hs[j as usize]);
        i
    }
}

fn find(p: &mut [u32], mut x: u32) -> u32 {
    while p[x as usize] != x {
        let g = p[p[x as usize] as usize];
        p[x as usize] = g;
        x = g;
    }
    x
}

const _: () = assert!(std::mem::size_of::<Ev>() == 64);
const _: () = assert!(std::mem::size_of::<Sv>() == 32);

struct SendPtr<T>(*mut T);
unsafe impl<T> Send for SendPtr<T> {}
unsafe impl<T> Sync for SendPtr<T> {}

fn build_links(files: &[S]) -> (LinkTable, usize) {
    let chunks = make_chunks::<2>(files, &["user_id_a", "user_id_b"]);
    // parse, and scatter each side by hash partition: (id, hash, slot = pair*2+side)
    let parsed: Vec<(usize, Vec<Vec<(Small, u64, u32)>>)> = chunks
        .par_iter()
        .map(|c| {
            let mut v: Vec<Vec<(Small, u64, u32)>> = (0..PP).map(|_| Vec::new()).collect();
            let mut n = 0u32;
            scan::<2>(c.data, |raw| {
                let [a, b] = permute(raw, &c.map);
                if !a.is_empty() && !b.is_empty() {
                    let end = c.data.as_ptr_range().end;
                    let (sa, sb) = (Small::new_in(a, end), Small::new_in(b, end));
                    let ha = sa.h();
                    let hb_ = sb.h();
                    v[upart(ha)].push((sa, ha, n * 2));
                    v[upart(hb_)].push((sb, hb_, n * 2 + 1));
                    n += 1;
                }
            });
            (n as usize, v)
        })
        .collect();
    let mut chunk_base = Vec::with_capacity(parsed.len());
    let mut npairs = 0usize;
    for (n, _) in &parsed {
        chunk_base.push(npairs);
        npairs += n;
    }
    // per side: partition << 24 | local index in that partition
    let mut local = vec![0u32; npairs * 2];
    let lp = SendPtr(local.as_mut_ptr());
    let lp = &lp;
    let mut parts: Vec<IdPart> = (0..PP)
        .into_par_iter()
        .map(|p| {
            let mut part = IdPart::new();
            for (ci, (_, v)) in parsed.iter().enumerate() {
                let base = (chunk_base[ci] * 2) as u32;
                for &(k, h, slot) in &v[p] {
                    let i = part.intern(k, h);
                    // SAFETY: every slot is written by exactly one partition
                    unsafe { *lp.0.add((base + slot) as usize) = ((p as u32) << 24) | i };
                }
            }
            part
        })
        .collect();
    drop(parsed);
    let mut pbase = vec![0u32; PP + 1];
    for p in 0..PP {
        pbase[p + 1] = pbase[p] + parts[p].keys.len() as u32;
    }
    let n = pbase[PP] as usize;
    vlog(&format!("links: parsed + interned {n} ids, {npairs} pairs"));
    let mut parent: Vec<u32> = (0..n as u32).collect();
    for k in 0..npairs {
        let (x, y) = (local[2 * k], local[2 * k + 1]);
        let a = pbase[(x >> 24) as usize] + (x & 0xFF_FFFF);
        let b = pbase[(y >> 24) as usize] + (y & 0xFF_FFFF);
        let ra = find(&mut parent, a);
        let rb = find(&mut parent, b);
        if ra != rb {
            if ra < rb {
                parent[rb as usize] = ra;
            } else {
                parent[ra as usize] = rb;
            }
        }
    }
    drop(local);
    vlog("links: union-find done");
    let mut root = vec![0u32; n];
    for i in 0..n {
        root[i] = find(&mut parent, i as u32);
    }
    // name = byte-wise smallest ID of the component
    vlog("links: roots done");
    let mut name: Vec<Option<Small>> = vec![None; n];
    for (p, part) in parts.iter().enumerate() {
        let b = pbase[p] as usize;
        for (i, k) in part.keys.iter().enumerate() {
            let r = root[b + i] as usize;
            match &name[r] {
                Some(cur) if cur.as_bytes() <= k.as_bytes() => {}
                _ => name[r] = Some(*k),
            }
        }
    }
    parts.par_iter_mut().enumerate().for_each(|(p, part)| {
        let b = pbase[p] as usize;
        let m = part.keys.len();
        part.name = (0..m).map(|i| name[root[b + i] as usize].unwrap()).collect();
    });
    drop(root);
    drop(name);
    vlog("links: names done");
    let t = LinkTable::build(&parts, n);
    vlog("links: table built");
    (t, n)
}

/// Flat open-addressing table of every linked ID -> (person name, name hash).
/// 32-byte slots (two per cache line); callers prefetch.
#[repr(C, align(32))]
#[derive(Clone, Copy)]
struct LSlot {
    key: Small,
    name: Small,
}

pub struct LinkTable {
    slots: &'static [LSlot],
    mask: usize,
}

impl LinkTable {
    fn build(parts: &[IdPart], n: usize) -> LinkTable {
        let cap = (n * 2).next_power_of_two().max(64);
        let bytes = cap * std::mem::size_of::<LSlot>();
        let m = memmap2::MmapMut::map_anon(bytes).expect("anon map");
        let _ = m.advise(memmap2::Advice::HugePage);
        let m: &'static mut memmap2::MmapMut = Box::leak(Box::new(m));
        // SAFETY: zeroed memory is a valid array of empty slots (key length 0)
        let slots: &'static mut [LSlot] = unsafe { std::slice::from_raw_parts_mut(m.as_mut_ptr() as *mut LSlot, cap) };
        let mask = cap - 1;
        for part in parts {
            for i in 0..part.keys.len() {
                let h = part.hashes[i];
                let mut idx = (h as usize) & mask;
                while !slots[idx].key.is_empty() {
                    idx = (idx + 1) & mask;
                }
                slots[idx] = LSlot { key: part.keys[i], name: part.name[i] };
            }
        }
        LinkTable { slots, mask }
    }
    #[inline(always)]
    fn prefetch(&self, h: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::*;
            _mm_prefetch(self.slots.as_ptr().add((h as usize) & self.mask) as *const i8, _MM_HINT_T0);
        }
    }
    /// Person (name, name hash) of a user ID; an unlinked ID is its own person.
    #[inline(always)]
    fn person(&self, user: Small, h: u64) -> (Small, u64) {
        let mut idx = (h as usize) & self.mask;
        loop {
            let s = &self.slots[idx];
            if s.key.is_empty() {
                return (user, h);
            }
            if s.key == user {
                return (s.name, s.name.h());
            }
            idx = (idx + 1) & self.mask;
        }
    }
}

// ---------------------------------------------------------------- records

#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct Ev { // exactly one cache line
    h: u64,
    ingest: i64,
    ts: i64,
    user: Small,
    eid: Small,
    price: u32,
    cat: u16,
    kind: u8, // 0 invalid, 1 view, 2 cart, 3 purchase
    dead: u8,
}

/// Sort key: 24 bits of the person hash, then 40 bits of time relative to
/// window_start - 7 days. Sorting by it groups persons (hash collisions are
/// fixed up afterwards) and orders each person's records by time.
const TS_BITS: u32 = 40;
const TS_MASK: u64 = (1 << TS_BITS) - 1;

#[inline(always)]
fn key(ph: u64, ts_rel: i64) -> u64 {
    ((ph & 0xFF_FFFF) << TS_BITS) | (ts_rel as u64 & TS_MASK)
}

#[derive(Clone, Copy)]
struct Sv {
    // 32 bytes
    k: u64,
    person: Small,
    price: u32, // 0 unless purchase
    cat: u16,
}

/// Two event records in one cache line, written with non-temporal stores.
/// An odd record out is padded with SV_PAD (price u32::MAX never occurs).
#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct SvPair([Sv; 2]);
const SV_PAD: Sv = Sv { k: 0, person: Small::EMPTY, price: u32::MAX, cat: 0 };

/// Per-destination staging so every store to memory is a full cache line.
struct PairScatter {
    pending: Vec<Sv>,
    has: Vec<bool>,
}

impl PairScatter {
    fn new(n: usize) -> Self {
        PairScatter { pending: vec![SV_PAD; n], has: vec![false; n] }
    }
    #[inline(always)]
    fn push(&mut self, out: &mut [Vec<SvPair>], dst: usize, e: Sv) {
        if !self.has[dst] {
            self.pending[dst] = e;
            self.has[dst] = true;
            return;
        }
        self.has[dst] = false;
        let pair = SvPair([self.pending[dst], e]);
        let v = &mut out[dst];
        if v.len() == v.capacity() {
            v.reserve(v.capacity().max(64));
        }
        #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
        unsafe {
            use std::arch::x86_64::*;
            let d = v.as_mut_ptr().add(v.len()) as *mut __m256i;
            let s = &pair as *const SvPair as *const __m256i;
            _mm256_stream_si256(d, _mm256_load_si256(s));
            _mm256_stream_si256(d.add(1), _mm256_load_si256(s.add(1)));
            v.set_len(v.len() + 1);
        }
        #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
        v.push(pair);
    }
    fn finish(self, out: &mut [Vec<SvPair>]) {
        for (dst, &h) in self.has.iter().enumerate() {
            if h {
                out[dst].push(SvPair([self.pending[dst], SV_PAD]));
            }
        }
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_sfence();
        }
    }
}

#[derive(Clone, Copy)]
struct Ck {
    k: u64,
    person: Small,
    cid: S,
    camp: u16,
}

#[derive(Clone, Copy)]
struct CkRaw {
    uh: u64,
    ts: i64,
    user: Small,
    cid: S,
    camp: u16,
}

struct Win {
    ws: i64,
    we: i64,
}

// ---------------------------------------------------------------- events

/// 1 view, 2 cart, 3 purchase, 0 anything else (exact, case-sensitive).
#[inline(always)]
fn event_kind(et: &[u8]) -> u8 {
    match et.len() {
        4 => {
            let w = u32::from_le_bytes(et.try_into().unwrap());
            if w == u32::from_le_bytes(*b"view") {
                1
            } else if w == u32::from_le_bytes(*b"cart") {
                2
            } else {
                0
            }
        }
        8 => (u64::from_le_bytes(et.try_into().unwrap()) == u64::from_le_bytes(*b"purchase")) as u8 * 3,
        _ => 0,
    }
}

fn parse_event_chunk(out: &mut [Vec<Ev>], c: &Chunk, w: &Win, prods: &Products) {
    let end = c.data.as_ptr_range().end;
    scan::<7>(c.data, |raw| {
        let [eid, user, ts, ingest, et, prod, price] = permute(raw, &c.map);
        if eid.is_empty() {
            return;
        }
        let se = Small::new_in(eid, end);
        let h = se.h();
        let ingest_ms = parse_ts(ingest);
        let mut kind = 0u8;
        let mut tsv = 0i64;
        let mut pr = 0u32;
        let mut cat = 0u16;
        let mut usr = Small::new(b"");
        if let (Some(ing), Some(t)) = (ingest_ms, parse_ts(ts)) {
            if !user.is_empty() && t >= w.ws && t < w.we && !prod.is_empty() && ing - t <= LATE {
                let k = event_kind(et);
                if k == 3 {
                    if let Some(p) = parse_price(price) {
                        pr = p;
                        cat = match prods.map.get(Small::new(prod)) {
                            Some(i) => prods.cat_of[i as usize],
                            None => prods.unknown,
                        };
                        kind = 3;
                    }
                } else {
                    kind = k;
                }
                if kind != 0 {
                    tsv = t;
                    usr = Small::new_in(user, end);
                }
            }
        }
        push_nt(&mut out[(h >> (64 - DB_BITS)) as usize], Ev {
            h,
            ingest: ingest_ms.unwrap_or(i64::MAX),
            ts: tsv,
            user: usr,
            eid: se,
            price: pr,
            cat,
            kind,
            dead: 0,
        });
    });
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_sfence();
    }
}

/// Append one 64-byte record with non-temporal stores (no read-for-ownership of the line).
#[inline(always)]
fn push_nt(v: &mut Vec<Ev>, e: Ev) {
    if v.len() == v.capacity() {
        v.reserve(v.capacity().max(64));
    }
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        let dst = v.as_mut_ptr().add(v.len()) as *mut __m256i;
        let src = &e as *const Ev as *const __m256i;
        _mm256_stream_si256(dst, _mm256_load_si256(src));
        _mm256_stream_si256(dst.add(1), _mm256_load_si256(src.add(1)));
        v.set_len(v.len() + 1);
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    v.push(e);
}

/// Dedup one bucket: per event_id keep the copy with the smallest ingest_ts,
/// then emit valid survivors by user partition.
fn dedup_bucket(mut parts: Vec<Vec<Ev>>, links: &LinkTable, base: i64) -> Vec<Vec<SvPair>> {
    let total: usize = parts.iter().map(|p| p.len()).sum();
    let mut table: HashTable<(u64, u32, u32)> = HashTable::with_capacity(total);
    let ptrs: Vec<*mut Ev> = parts.iter_mut().map(|v| v.as_mut_ptr()).collect();
    for (j, v) in parts.iter().enumerate() {
        for i in 0..v.len() {
            // SAFETY: indices are in bounds; e and cur are never the same record
            let e = unsafe { &mut *ptrs[j].add(i) };
            let h = e.h;
            let th = h.rotate_left(32); // keys in a bucket share their top bits: move varying bits into the tag
            let eid = e.eid;
            let found = table
                .find_mut(th, |&(hh, jj, ii)| hh == h && unsafe { (*ptrs[jj as usize].add(ii as usize)).eid } == eid);
            match found {
                Some(slot) => {
                    let cur = unsafe { &mut *ptrs[slot.1 as usize].add(slot.2 as usize) };
                    if e.ingest < cur.ingest {
                        cur.dead = 1;
                        *slot = (h, j as u32, i as u32);
                    } else {
                        e.dead = 1;
                    }
                }
                None => {
                    table.insert_unique(th, (h, j as u32, i as u32), |x| x.0.rotate_left(32));
                }
            }
        }
    }
    drop(table);
    const D: usize = 16; // prefetch distance
    let cap = (total / PP + total / PP / 8) / 2 + 16;
    let mut out: Vec<Vec<SvPair>> = (0..PP).map(|_| Vec::with_capacity(cap)).collect();
    let mut sc = PairScatter::new(PP);
    let mut live: Vec<(u64, u32)> = Vec::new();
    for v in &parts {
        live.clear();
        for (i, e) in v.iter().enumerate() {
            if e.dead == 0 && e.kind != 0 {
                live.push((e.user.h(), i as u32));
            }
        }
        for k in 0..live.len() {
            if k + D < live.len() {
                links.prefetch(live[k + D].0);
            }
            let (uh, i) = live[k];
            let e = &v[i as usize];
            let (person, ph) = links.person(e.user, uh);
            sc.push(&mut out, upart(ph), Sv { k: key(ph, e.ts - base), person, price: if e.kind == 3 { e.price } else { 0 }, cat: e.cat });
        }
    }
    sc.finish(&mut out);
    out
}

fn parse_click_chunk(out: &mut [Vec<Ck>], c: &Chunk, w: &Win, camps: &Campaigns, links: &LinkTable) {
    let end = c.data.as_ptr_range().end;
    const B: usize = 16;
    let lo = w.ws - WEEK;
    let mut batch: [Option<CkRaw>; B] = [None; B];
    let mut nb = 0usize;
    let flush = |out: &mut [Vec<Ck>], batch: &mut [Option<CkRaw>; B], nb: usize| {
        for slot in batch.iter_mut().take(nb) {
            let c = slot.take().unwrap();
            let (person, ph) = links.person(c.user, c.uh);
            out[upart(ph)].push(Ck { k: key(ph, c.ts - lo), person, cid: c.cid, camp: c.camp });
        }
    };
    scan::<4>(c.data, |raw| {
        let [cid, user, camp, ts] = permute(raw, &c.map);
        if cid.is_empty() || user.is_empty() {
            return;
        }
        let Some(t) = parse_ts(ts) else { return };
        if t < lo || t >= w.we {
            return;
        }
        let Some(ci) = camps.map.get(Small::new_in(camp, end)) else { return };
        let su = Small::new_in(user, end);
        let uh = su.h();
        links.prefetch(uh);
        batch[nb] = Some(CkRaw { uh, ts: t, user: su, cid, camp: ci as u16 });
        nb += 1;
        if nb == B {
            flush(out, &mut batch, nb);
            nb = 0;
        }
    });
    flush(out, &mut batch, nb);
}

// ---------------------------------------------------------------- sessions and aggregation

pub struct UserRow {
    pub name: Small,
    pub day: i32,
    pub sessions: u32,
    pub events: u64,
    pub revenue: u64,
    pub top: i32, // -1 none
}

struct Acc {
    rows: Vec<UserRow>,
    camp: Vec<u64>, // [(camp * ndays + day) * 4 + k], k: sessions, purchasing, revenue, assisted
    counts: Vec<u32>,
    pos: Vec<u32>,
    active: Vec<u16>,
    catsum: Vec<u64>,
}

struct Ctx {
    base: i64, // window_start - 7 days: origin of the relative times in keys
    ncamp: usize,
    ndays: usize,
    ws_day: i64,
    ncat: usize,
}

impl Acc {
    fn new(cx: &Ctx) -> Acc {
        Acc {
            rows: Vec::new(),
            camp: vec![0; (cx.ncamp + 1) * cx.ndays * 4],
            counts: vec![0; cx.ncamp],
            pos: vec![0; cx.ncamp],
            active: Vec::new(),
            catsum: vec![0; cx.ncat],
        }
    }
    #[inline(always)]
    fn add(&mut self, c: u16) {
        let ci = c as usize;
        if self.counts[ci] == 0 {
            self.pos[ci] = self.active.len() as u32;
            self.active.push(c);
        }
        self.counts[ci] += 1;
    }
    #[inline(always)]
    fn remove(&mut self, c: u16) {
        let ci = c as usize;
        self.counts[ci] -= 1;
        if self.counts[ci] == 0 {
            let p = self.pos[ci] as usize;
            let last = *self.active.last().unwrap();
            self.active[p] = last;
            self.pos[last as usize] = p as u32;
            self.active.pop();
        }
    }
}

/// After sorting by (hash, ...), make records of one hash that belong to different
/// persons contiguous per person (stable, so time order is kept). Practically never runs.
fn fix_collisions<T>(v: &mut [T], ph: impl Fn(&T) -> u64, pb: impl Fn(&T) -> Small) {
    let n = v.len();
    let mut i = 0;
    while i < n {
        let h = ph(&v[i]);
        let first = pb(&v[i]);
        let mut j = i + 1;
        let mut diff = false;
        while j < n && ph(&v[j]) == h {
            if !diff && pb(&v[j]) != first {
                diff = true;
            }
            j += 1;
        }
        if diff {
            v[i..j].sort_by(|a, b| pb(a).cmp(&pb(b)));
        }
        i = j;
    }
}

// ---------------------------------------------------------------- radix sort by key
//
// One counting pass and one scatter by the top RB bits of the key, then a small
// sort inside each bucket. The result is sorted by the full key, the same order
// a comparison sort gives, at a fraction of the cost.

const RB: u32 = 12;
const NRB: usize = 1 << RB;

#[inline(always)]
fn rbucket(k: u64) -> usize {
    (k >> (64 - RB)) as usize
}

/// Insertion sort for the short runs, `big` (a library sort) for longer ones.
#[inline(always)]
fn small_sort<T: Copy>(v: &mut [T], less: impl Fn(&T, &T) -> bool, big: impl Fn(&mut [T])) {
    if v.len() > 48 {
        big(v);
        return;
    }
    for i in 1..v.len() {
        let x = v[i];
        let mut j = i;
        while j > 0 && less(&x, &v[j - 1]) {
            v[j] = v[j - 1];
            j -= 1;
        }
        v[j] = x;
    }
}

fn starts_from_counts(cnt: &mut [u32]) -> usize {
    let mut s = 0u32;
    for c in cnt.iter_mut() {
        let n = *c;
        *c = s;
        s += n;
    }
    s as usize
}

/// Concatenate the per-job event pairs, sorted by key.
fn sorted_events(parts: Vec<Vec<SvPair>>) -> Vec<Sv> {
    let mut cnt = vec![0u32; NRB];
    for p in &parts {
        for pr in p {
            cnt[rbucket(pr.0[0].k)] += 1;
            if pr.0[1].price != u32::MAX {
                cnt[rbucket(pr.0[1].k)] += 1;
            }
        }
    }
    let n = starts_from_counts(&mut cnt);
    let starts = cnt.clone();
    let mut out: Vec<Sv> = Vec::with_capacity(n);
    let dst = out.as_mut_ptr();
    for p in &parts {
        for pr in p {
            let a = pr.0[0];
            let ba = rbucket(a.k);
            // SAFETY: the counting pass sized every bucket; each slot is written once
            unsafe { dst.add(cnt[ba] as usize).write(a) };
            cnt[ba] += 1;
            let b = pr.0[1];
            if b.price != u32::MAX {
                let bb = rbucket(b.k);
                unsafe { dst.add(cnt[bb] as usize).write(b) };
                cnt[bb] += 1;
            }
        }
    }
    drop(parts);
    unsafe { out.set_len(n) };
    for b in 0..NRB {
        let e = if b + 1 < NRB { starts[b + 1] as usize } else { n };
        small_sort(&mut out[starts[b] as usize..e], |x, y| x.k < y.k, |s| s.sort_unstable_by_key(|x| x.k));
    }
    out
}

/// Concatenate the per-job clicks, sorted by key and then click_id descending
/// (so the last click at or before a session start is the winner).
fn sorted_clicks(parts: Vec<Vec<Ck>>) -> Vec<Ck> {
    let mut cnt = vec![0u32; NRB];
    for p in &parts {
        for c in p {
            cnt[rbucket(c.k)] += 1;
        }
    }
    let n = starts_from_counts(&mut cnt);
    let starts = cnt.clone();
    let mut out: Vec<Ck> = Vec::with_capacity(n);
    let dst = out.as_mut_ptr();
    for p in &parts {
        for &c in p {
            let b = rbucket(c.k);
            // SAFETY: as in sorted_events
            unsafe { dst.add(cnt[b] as usize).write(c) };
            cnt[b] += 1;
        }
    }
    drop(parts);
    unsafe { out.set_len(n) };
    for b in 0..NRB {
        let e = if b + 1 < NRB { starts[b + 1] as usize } else { n };
        small_sort(&mut out[starts[b] as usize..e], |x, y| x.k < y.k || (x.k == y.k && x.cid > y.cid), |s| {
            s.sort_unstable_by(|x, y| x.k.cmp(&y.k).then_with(|| y.cid.cmp(x.cid)))
        });
    }
    out
}

fn process_partition(acc: &mut Acc, cx: &Ctx, evs: &mut [Sv], cks: &mut [Ck]) {
    // evs and cks arrive sorted by key (clicks: then click_id descending)
    fix_collisions(evs, |e| e.k >> TS_BITS, |e| e.person);
    fix_collisions(cks, |c| c.k >> TS_BITS, |c| c.person);

    let ndays = cx.ndays;
    let organic = cx.ncamp;
    let n = evs.len();
    let nc = cks.len();
    let mut cj = 0usize;
    let mut i = 0usize;
    while i < n {
        let ph = evs[i].k >> TS_BITS;
        let person = evs[i].person;
        let mut b = i + 1;
        while b < n && evs[b].k >> TS_BITS == ph && evs[b].person == person {
            b += 1;
        }
        while cj < nc && (cks[cj].k >> TS_BITS < ph || (cks[cj].k >> TS_BITS == ph && cks[cj].person < person)) {
            cj += 1;
        }
        let c0 = cj;
        let mut c1 = cj;
        while c1 < nc && cks[c1].k >> TS_BITS == ph && cks[c1].person == person {
            c1 += 1;
        }
        cj = c1;

        let mut lo = c0;
        let mut hi = c0;
        let mut cur_day: i32 = i32::MIN;
        let mut u_sess = 0u32;
        let mut u_ev = 0u64;
        let mut u_rev = 0u64;

        let mut k = i;
        while k < b {
            let start = (evs[k].k & TS_MASK) as i64;
            let day = ((start + cx.base).div_euclid(DAY) - cx.ws_day) as i32;
            if day != cur_day {
                if cur_day != i32::MIN {
                    flush_user(acc, cx, person, cur_day, u_sess, u_ev, u_rev);
                }
                cur_day = day;
                u_sess = 0;
                u_ev = 0;
                u_rev = 0;
            }
            let mut prev = start;
            let mut m = k;
            let mut rev = 0u64;
            while m < b && (evs[m].k & TS_MASK) as i64 - prev <= GAP {
                let e = &evs[m];
                prev = (e.k & TS_MASK) as i64;
                if e.price != 0 {
                    rev += e.price as u64;
                    acc.catsum[e.cat as usize] += e.price as u64;
                }
                m += 1;
            }
            // clicks in [start - 7d, start]
            while hi < c1 && ((cks[hi].k & TS_MASK) as i64) <= start {
                let c = cks[hi].camp;
                acc.add(c);
                hi += 1;
            }
            while lo < hi && ((cks[lo].k & TS_MASK) as i64) < start - WEEK {
                let c = cks[lo].camp;
                acc.remove(c);
                lo += 1;
            }
            let credited = if lo < hi { cks[hi - 1].camp as usize } else { organic };
            let d = day as usize;
            let base = (credited * ndays + d) * 4;
            acc.camp[base] += 1;
            if rev > 0 {
                acc.camp[base + 1] += 1;
                acc.camp[base + 2] += rev;
            }
            for ai in 0..acc.active.len() {
                let c = acc.active[ai] as usize;
                acc.camp[(c * ndays + d) * 4 + 3] += 1;
            }
            u_sess += 1;
            u_ev += (m - k) as u64;
            u_rev += rev;
            k = m;
        }
        flush_user(acc, cx, person, cur_day, u_sess, u_ev, u_rev);
        while lo < hi {
            let c = cks[lo].camp;
            acc.remove(c);
            lo += 1;
        }
        i = b;
    }
}

#[inline(always)]
fn flush_user(acc: &mut Acc, cx: &Ctx, name: Small, day: i32, sessions: u32, events: u64, revenue: u64) {
    let mut top = -1i32;
    if revenue > 0 {
        let mut best = 0u64;
        for c in 0..cx.ncat {
            let s = acc.catsum[c];
            if s > best {
                best = s;
                top = c as i32;
            }
            acc.catsum[c] = 0;
        }
    }
    acc.rows.push(UserRow { name, day: day + cx.ws_day as i32, sessions, events, revenue, top });
}

// ---------------------------------------------------------------- main

fn read_manifest(p: &Path) -> Win {
    let s = std::fs::read_to_string(p).expect("manifest.json");
    let get = |key: &str| -> i64 {
        let k = format!("\"{key}\"");
        let at = s.find(&k).unwrap_or_else(|| panic!("{key} missing")) + k.len();
        let rest = &s[at..];
        let q1 = rest.find('"').unwrap() + 1;
        let q2 = rest[q1..].find('"').unwrap() + q1;
        parse_ts(rest[q1..q2].as_bytes()).unwrap_or_else(|| panic!("bad {key}"))
    };
    Win { ws: get("window_start"), we: get("window_end") }
}

/// Run `f` over all chunks in parallel with exactly one accumulator per pool thread.
fn per_thread<A: Send, C: Sync>(chunks: &[C], nthreads: usize, init: impl Fn() -> A + Sync, f: impl Fn(&mut A, &C) + Sync) -> Vec<A> {
    use std::sync::Mutex;
    let slots: Vec<Mutex<Option<A>>> = (0..nthreads + 1).map(|_| Mutex::new(None)).collect();
    chunks.par_iter().for_each(|c| {
        let t = rayon::current_thread_index().unwrap_or(nthreads).min(nthreads);
        let mut g = slots[t].lock().unwrap();
        let acc = g.get_or_insert_with(&init);
        f(acc, c);
    });
    slots.into_iter().filter_map(|m| m.into_inner().unwrap()).collect()
}

/// Like per_thread, but consumes owned work items (so their memory is freed as they finish).
fn per_thread_owned<A: Send, W: Send>(items: Vec<W>, nthreads: usize, init: impl Fn() -> A + Sync, f: impl Fn(&mut A, W) + Sync) -> Vec<A> {
    use std::sync::Mutex;
    let slots: Vec<Mutex<Option<A>>> = (0..nthreads + 1).map(|_| Mutex::new(None)).collect();
    items.into_par_iter().with_max_len(1).for_each(|w| {
        let t = rayon::current_thread_index().unwrap_or(nthreads).min(nthreads);
        let mut g = slots[t].lock().unwrap();
        let acc = g.get_or_insert_with(&init);
        f(acc, w);
    });
    slots.into_iter().filter_map(|m| m.into_inner().unwrap()).collect()
}

/// Move [job][bucket] vectors into [bucket][job].
fn transpose<T>(v: Vec<Vec<Vec<T>>>, nb: usize) -> Vec<Vec<Vec<T>>> {
    let mut out: Vec<Vec<Vec<T>>> = (0..nb).map(|_| Vec::with_capacity(v.len())).collect();
    for job in v {
        for (b, x) in job.into_iter().enumerate() {
            out[b].push(x);
        }
    }
    out
}

fn main() {
    let t0 = Instant::now();
    let _ = T0.set(t0);
    VERBOSE.store(std::env::var("SS_VERBOSE").is_ok(), std::sync::atomic::Ordering::Relaxed);
    let verbose = std::env::var("SS_VERBOSE").is_ok();
    let log = |m: &str| {
        if verbose {
            let st = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
            let g = |k: &str| st.lines().find(|l| l.starts_with(k)).map(|l| l.split_whitespace().nth(1).unwrap_or("0").parse::<u64>().unwrap_or(0) / 1024).unwrap_or(0);
            eprintln!("[{:7.3}s] {m}   anon {} MB, file {} MB, peak {} MB", t0.elapsed().as_secs_f64(), g("RssAnon"), g("RssFile"), g("VmHWM"))
        }
    };
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let outdir = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let ncpu: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8));
    rayon::ThreadPoolBuilder::new().num_threads(ncpu.max(1)).build_global().ok();

    let w = read_manifest(&data.join("manifest.json"));
    assert!(w.we - (w.ws - WEEK) < (1i64 << TS_BITS), "window too long for 40-bit relative times");
    let prods = load_products(map_file(&data.join("products.csv")));
    let camps = load_campaigns(map_file(&data.join("campaigns.csv")));

    let ev_files = list_csv(&data.join("events"));
    let ck_files = list_csv(&data.join("clicks"));
    let ln_files = list_csv(&data.join("links"));
    let ev_chunks = make_chunks::<7>(
        &ev_files,
        &["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"],
    );
    let ck_chunks = make_chunks::<4>(&ck_files, &["click_id", "user_id", "campaign_id", "ts"]);
    log("mapped");
    let nthreads = rayon::current_num_threads().max(1);
    let ev_bytes: usize = ev_chunks.iter().map(|c| c.data.len()).sum();
    let ck_bytes: usize = ck_chunks.iter().map(|c| c.data.len()).sum();
    // expected records per thread per bucket, with headroom
    let ev_cap = ev_bytes / 70 / nthreads / DB + 64;
    let ck_cap = ck_bytes / 40 / nthreads / PP + 32;

    // Phase 1: links (one thread) while all other threads parse events.
    let ((links, nlink), ev_jobs) = rayon::join(
        || build_links(&ln_files),
        || {
            let r = per_thread(&ev_chunks, nthreads, || (0..DB).map(|_| Vec::with_capacity(ev_cap)).collect::<Vec<Vec<Ev>>>(), |out, c| {
                parse_event_chunk(out, c, &w, &prods)
            });
            vlog("events: all chunks parsed");
            r
        },
    );
    log(&format!("parsed events: {} chunks, {} link ids", ev_chunks.len(), nlink));

    // Phase 2: dedup events and parse clicks; both resolve persons and bucket by person.
    let ev_buckets = transpose(ev_jobs, DB);
    let (ev_pp, ck_jobs) = rayon::join(
        || ev_buckets.into_par_iter().map(|b| dedup_bucket(b, &links, w.ws - WEEK)).collect::<Vec<Vec<Vec<SvPair>>>>(),
        || {
            per_thread(&ck_chunks, nthreads, || (0..PP).map(|_| Vec::with_capacity(ck_cap)).collect::<Vec<Vec<Ck>>>(), |out, c| {
                parse_click_chunk(out, c, &w, &camps, &links)
            })
        },
    );
    log("dedup + clicks");

    // Phase 3: sessions.
    let ws_day = w.ws.div_euclid(DAY);
    let ndays = ((w.we - 1).div_euclid(DAY) - ws_day + 1).max(1) as usize;
    let cx = Ctx { base: w.ws - WEEK, ncamp: camps.channel.len(), ndays, ws_day, ncat: prods.cats.len() };
    let ev_by_pp = transpose(ev_pp, PP);
    let ck_by_pp = transpose(ck_jobs, PP);
    let accs: Vec<Acc> = per_thread_owned(
        ev_by_pp.into_iter().zip(ck_by_pp).collect(),
        nthreads,
        || Acc::new(&cx),
        |acc, (ev_parts, ck_parts): (Vec<Vec<SvPair>>, Vec<Vec<Ck>>)| {
            let mut evs: Vec<Sv> = sorted_events(ev_parts);
            let mut cks: Vec<Ck> = sorted_clicks(ck_parts);
            process_partition(acc, &cx, &mut evs, &mut cks);
        },
    );
    log("sessions");

    // Phase 4: output.
    let mut camp = vec![0u64; (cx.ncamp + 1) * ndays * 4];
    for a in &accs {
        for (x, y) in camp.iter_mut().zip(a.camp.iter()) {
            *x += *y;
        }
    }
    let mut camp_rows: Vec<output::CampRow> = Vec::new();
    for c in 0..=cx.ncamp {
        for d in 0..ndays {
            let base = (c * ndays + d) * 4;
            if camp[base] > 0 || camp[base + 3] > 0 {
                let (id, ch): (S, S) =
                    if c == cx.ncamp { (b"organic", b"organic") } else { (camps.map.keys[c], camps.channel[c]) };
                camp_rows.push(output::CampRow {
                    id,
                    channel: ch,
                    day: (ws_day + d as i64) as i32,
                    sessions: camp[base],
                    purchasing: camp[base + 1],
                    revenue: camp[base + 2],
                    assisted: camp[base + 3],
                });
            }
        }
    }
    let ud = outdir.join("user_daily");
    let cd = outdir.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("create user_daily");
    std::fs::create_dir_all(&cd).expect("create campaign_daily");
    let cats = &prods.cats;
    rayon::join(
        || {
            accs.par_iter().enumerate().for_each(|(i, a)| {
                output::write_user(&ud.join(format!("part-{i:05}.parquet")), &a.rows, cats);
            })
        },
        || output::write_campaign(&cd.join("part-00000.parquet"), &camp_rows),
    );
    log(&format!(
        "written: {} user rows, {} campaign rows",
        accs.iter().map(|a| a.rows.len()).sum::<usize>(),
        camp_rows.len()
    ));
    std::process::exit(0);
}
