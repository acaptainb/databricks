// Files, CSV dialect and timestamps.
use crate::{S, CHUNK, MAX_PRICE};
use rayon::prelude::*;
use std::fs::File;
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------- files

pub fn map_file(p: &Path) -> S {
    let f = File::open(p).unwrap_or_else(|e| panic!("open {p:?}: {e}"));
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    if len == 0 {
        return &[];
    }
    let m = unsafe { memmap2::MmapOptions::new().map(&f) }.expect("mmap");
    #[cfg(unix)]
    {
        // Ask the kernel to start reading the whole file now (helps a cold page cache).
        let _ = m.advise(memmap2::Advice::Sequential);
        let _ = m.advise(memmap2::Advice::WillNeed);
    }
    let m: &'static memmap2::Mmap = Box::leak(Box::new(m));
    &m[..]
}

pub fn list_csv(dir: &Path) -> Vec<S> {
    let mut paths: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                let n = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                n.ends_with(".csv") && !n.starts_with('.') && p.is_file()
            })
            .collect(),
        Err(_) => Vec::new(),
    };
    paths.sort();
    paths.par_iter().map(|p| map_file(p)).collect()
}

// ---------------------------------------------------------------- CSV dialect

#[inline(always)]
pub fn unquote(f: S) -> Option<S> {
    if memchr::memchr(b'"', f).is_none() {
        return Some(f);
    }
    let n = f.len();
    if n >= 2 && f[0] == b'"' && f[n - 1] == b'"' && memchr::memchr(b'"', &f[1..n - 1]).is_none() {
        Some(&f[1..n - 1])
    } else {
        None
    }
}

/// Split a line into exactly N fields in file order. None if malformed.
#[inline(always)]
pub fn split<const N: usize>(line: S) -> Option<[S; N]> {
    let mut out: [S; N] = [&[]; N];
    let has_quote = memchr::memchr(b'"', line).is_some();
    let mut n = 0usize;
    let mut start = 0usize;
    let len = line.len();
    loop {
        let end = match memchr::memchr(b',', &line[start..]) {
            Some(p) => start + p,
            None => len,
        };
        if n >= N {
            return None;
        }
        let f = &line[start..end];
        out[n] = if has_quote { unquote(f)? } else { f };
        n += 1;
        if end == len {
            break;
        }
        start = end + 1;
    }
    if n == N {
        Some(out)
    } else {
        None
    }
}

#[inline(always)]
pub fn for_lines(data: S, mut f: impl FnMut(S)) {
    let mut s = 0usize;
    for nl in memchr::memchr_iter(b'\n', data) {
        f(&data[s..nl]);
        s = nl + 1;
    }
    if s < data.len() {
        f(&data[s..]);
    }
}

#[derive(Clone, Copy)]
pub struct Chunk {
    pub data: S,
    pub map: [u8; 8],
}

/// Column order from a header line; identity when the header is not recognised.
pub fn header_map<const N: usize>(header: S, names: &[&str; N]) -> [u8; 8] {
    let mut map = [0u8; 8];
    for i in 0..N {
        map[i] = i as u8;
    }
    if let Some(fields) = split::<N>(header) {
        let mut m = [0u8; 8];
        let mut ok = true;
        for (k, name) in names.iter().enumerate() {
            match fields.iter().position(|f| *f == name.as_bytes()) {
                Some(p) => m[k] = p as u8,
                None => ok = false,
            }
        }
        if ok {
            return m;
        }
    }
    map
}

pub fn make_chunks<const N: usize>(files: &[S], names: &[&str; N]) -> Vec<Chunk> {
    make_chunks_sized::<N>(files, names, CHUNK)
}

pub fn make_chunks_sized<const N: usize>(files: &[S], names: &[&str; N], chunk: usize) -> Vec<Chunk> {
    let mut out = Vec::new();
    for &f in files {
        let Some(nl) = memchr::memchr(b'\n', f) else { continue };
        let map = header_map::<N>(&f[..nl], names);
        let mut pos = nl + 1;
        while pos < f.len() {
            let mut end = (pos + chunk).min(f.len());
            if end < f.len() {
                end = match memchr::memchr(b'\n', &f[end..]) {
                    Some(p) => end + p + 1,
                    None => f.len(),
                };
            }
            out.push(Chunk { data: &f[pos..end], map });
            pos = end;
        }
    }
    // Big chunks first for better load balance.
    out.sort_by_key(|c| std::cmp::Reverse(c.data.len()));
    out
}

// ---------------------------------------------------------------- timestamps

#[inline(always)]
pub fn d2(b: &[u8], i: usize) -> Option<u32> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a < 10 && c < 10 {
        Some(a as u32 * 10 + c as u32)
    } else {
        None
    }
}

#[inline(always)]
pub const fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

#[inline(always)]
pub const fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let m = m as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

const Y0: i64 = 1970;
const NY: usize = 256; // fast-path years 1970..2226

const fn year_days_table() -> [i64; NY] {
    let mut t = [0i64; NY];
    let mut i = 0;
    while i < NY {
        t[i] = days_from_civil(Y0 + i as i64, 1, 1);
        i += 1;
    }
    t
}
static YEAR_DAYS: [i64; NY] = year_days_table();
const fn year_leap_table() -> [bool; NY] {
    let mut t = [false; NY];
    let mut i = 0;
    while i < NY {
        t[i] = is_leap(Y0 + i as i64);
        i += 1;
    }
    t
}
static YEAR_LEAP: [bool; NY] = year_leap_table();
static DIM: [[i64; 12]; 2] = [[31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31], [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]];
// days before each month, [leap][month-1]
static CUM: [[i64; 12]; 2] = [
    [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334],
    [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335],
];

/// Subtract '0' from the bytes selected by `m`; None unless each is an ASCII digit.
#[inline(always)]
fn swar_digits(w: u64, m: u64) -> Option<u64> {
    let x = w & m;
    let z = 0x3030_3030_3030_3030u64 & m;
    if x & (0xF0F0_F0F0_F0F0_F0F0 & m) != z {
        return None;
    }
    let d = x - z;
    if (d + (0x0606_0606_0606_0606 & m)) & 0xF0F0_F0F0_F0F0_F0F0 != 0 {
        return None;
    }
    Some(d)
}

#[inline(always)]
fn byte(d: u64, i: u32) -> i64 {
    ((d >> (8 * i)) & 0xFF) as i64
}

/// `YYYY-MM-DDTHH:MM:SS` (the first 19 bytes of `b`, which must hold at least 20) to epoch ms.
#[inline(always)]
pub fn parse_dt(b: &[u8]) -> Option<i64> {
    let a = u64::from_le_bytes(b[0..8].try_into().unwrap()); // YYYY-MM-
    let c = u64::from_le_bytes(b[8..16].try_into().unwrap()); // DDTHH:MM
    let e = u32::from_le_bytes(b[16..20].try_into().unwrap()) as u64; // :SS?
    if a & 0xFF00_00FF_0000_0000 != ((b'-' as u64) << 56 | (b'-' as u64) << 32)
        || c & 0x0000_FF00_00FF_0000 != ((b':' as u64) << 40 | (b'T' as u64) << 16)
        || e & 0xFF != b':' as u64
    {
        return None;
    }
    let da = swar_digits(a, 0x00FF_FF00_FFFF_FFFF)?;
    let dc = swar_digits(c, 0xFFFF_00FF_FF00_FFFF)?;
    let de = swar_digits(e, 0x00FF_FF00)?;
    let y = byte(da, 0) * 1000 + byte(da, 1) * 100 + byte(da, 2) * 10 + byte(da, 3);
    let mo = byte(da, 5) * 10 + byte(da, 6);
    let d = byte(dc, 0) * 10 + byte(dc, 1);
    let h = byte(dc, 3) * 10 + byte(dc, 4);
    let mi = byte(dc, 6) * 10 + byte(dc, 7);
    let s = byte(de, 1) * 10 + byte(de, 2);
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || s > 59 || d < 1 {
        return None;
    }
    let fast = (Y0..Y0 + NY as i64).contains(&y);
    let leap = if fast { YEAR_LEAP[(y - Y0) as usize] } else { is_leap(y) };
    if d > DIM[leap as usize][(mo - 1) as usize] {
        return None;
    }
    let days = if fast {
        YEAR_DAYS[(y - Y0) as usize] + CUM[leap as usize][(mo - 1) as usize] + d - 1
    } else {
        days_from_civil(y, mo as u32, d as u32)
    };
    Some(((days * 24 + h) * 60 + mi) * 60_000 + s * 1000)
}

#[inline(always)]
pub fn parse_digits_i64(b: &[u8]) -> Option<i64> {
    if b.is_empty() {
        return None;
    }
    let mut v: i64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(d as i64)?;
    }
    Some(v)
}

#[inline(always)]
pub fn all_digits(b: &[u8]) -> bool {
    b.iter().all(|c| c.is_ascii_digit())
}

/// True when all 8 bytes of `w` are ASCII digits.
#[inline(always)]
fn is8digits(w: u64) -> bool {
    (w & 0xF0F0_F0F0_F0F0_F0F0) == 0x3030_3030_3030_3030
        && (w.wrapping_add(0x0606_0606_0606_0606) & 0xF0F0_F0F0_F0F0_F0F0) == 0x3030_3030_3030_3030
}

/// Value of 8 ASCII digits loaded little-endian (first character most significant).
#[inline(always)]
fn digits8(w: u64) -> u64 {
    let v = (w & 0x0F0F_0F0F_0F0F_0F0F).wrapping_mul(2561) >> 8;
    let v = (v & 0x00FF_00FF_00FF_00FF).wrapping_mul(6_553_601) >> 16;
    (v & 0x0000_FFFF_0000_FFFF).wrapping_mul(42_949_672_960_001) >> 32
}

/// F3: digits only (leading zeros allowed). Up to 16 digits with SWAR; longer
/// strings fall back to the checked loop (too long for i64 -> None, which, like
/// an out-of-window value, drops the row).
#[inline(always)]
pub fn parse_f3(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 0 {
        return None;
    }
    if n <= 16 {
        let mut buf = [b'0'; 16];
        buf[16 - n..].copy_from_slice(b);
        let w0 = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let w1 = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        if !(is8digits(w0) & is8digits(w1)) {
            return None;
        }
        return Some((digits8(w0) * 100_000_000 + digits8(w1)) as i64);
    }
    if all_digits(b) {
        parse_digits_i64(b)
    } else {
        None
    }
}

/// Parse an F1, F2 or F3 timestamp to epoch ms. None for a bad one (window checked by caller).
#[inline(always)]
pub fn parse_ts(b: &[u8]) -> Option<i64> {
    match b.len() {
        20 if b[19] == b'Z' => parse_dt(b),
        25 if b[19] == b'+' || b[19] == b'-' => {
            let base = parse_dt(b)?;
            if b[22] != b':' {
                return None;
            }
            let oh = d2(b, 20)?;
            let om = d2(b, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let off = (oh as i64 * 60 + om as i64) * 60_000;
            Some(if b[19] == b'+' { base - off } else { base + off })
        }
        _ => {
            parse_f3(b)
        }
    }
}

/// Purchase price: ASCII digits only, value 1..=10,000,000.
#[inline(always)]
pub fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > MAX_PRICE {
            // keep scanning for non-digits is unnecessary: too big is invalid either way
            return None;
        }
    }
    if v >= 1 {
        Some(v as u32)
    } else {
        None
    }
}


// ---------------------------------------------------------------- fast block scanner

/// Bitmasks of ',', '\n' and '"' for 64 bytes at p.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    let a = _mm256_loadu_si256(p as *const __m256i);
    let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let mk = |v: __m256i, c: i8| -> u32 { _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(c))) as u32 };
    let c = mk(a, b',' as i8) as u64 | ((mk(b, b',' as i8) as u64) << 32);
    let n = mk(a, b'\n' as i8) as u64 | ((mk(b, b'\n' as i8) as u64) << 32);
    let q = mk(a, b'"' as i8) as u64 | ((mk(b, b'"' as i8) as u64) << 32);
    (c, n, q)
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    let (mut c, mut n, mut q) = (0u64, 0u64, 0u64);
    for i in 0..64 {
        let b = *p.add(i);
        c |= ((b == b',') as u64) << i;
        n |= ((b == b'\n') as u64) << i;
        q |= ((b == b'"') as u64) << i;
    }
    (c, n, q)
}

/// Build the N fields of one line from its comma positions; applies the quote rules.
#[inline(always)]
fn finish_line<const N: usize>(data: S, start: usize, end: usize, commas: &[u32; 8], nc: usize, quotes: u32) -> Option<[S; N]> {
    if nc != N - 1 {
        return None;
    }
    let mut out: [S; N] = [&[]; N];
    let mut s = start;
    for k in 0..N {
        let e = if k < N - 1 { commas[k] as usize } else { end };
        out[k] = unsafe { data.get_unchecked(s..e) };
        s = e + 1;
    }
    if quotes != 0 {
        // Branch-free: strip one quote pair from each field that starts and
        // ends with a quote (and has at least 2 bytes); count what was stripped.
        let mut k2 = 0u32;
        let base = data.as_ptr() as usize;
        for f in out.iter_mut() {
            let n = f.len();
            let p = f.as_ptr();
            let last = n.saturating_sub(1);
            // SAFETY: when n == 0 both reads hit p[0], which is the separator
            // after this field, or the byte before the end of data; reads are
            // clamped to the data range below.
            let lim = base + data.len() - 1;
            let a = unsafe { *((p as usize).min(lim) as *const u8) } == b'"';
            let b = unsafe { *((p as usize + last).min(lim) as *const u8) } == b'"';
            let q = ((n >= 2) & a & b) as usize;
            *f = unsafe { std::slice::from_raw_parts(p.add(q), n - 2 * q) };
            k2 += 2 * q as u32;
        }
        if k2 != quotes {
            return None;
        }
    }
    Some(out)
}

/// Call `cb` with the fields (file order) of every well-formed line of `data`.
/// Malformed lines (wrong field count, stray quotes, empty) are skipped.
#[inline(always)]
pub fn scan<const N: usize>(data: S, mut cb: impl FnMut([S; N])) {
    let len = data.len();
    let ptr = data.as_ptr();
    let mut line_start = 0usize;
    let mut commas = [0u32; 8];
    let mut nc = 0usize;
    let mut qacc = 0u32;
    let mut o = 0usize;
    let mut tail = [0u8; 64];
    while o < len {
        let (mut c, mut n, mut q) = if o + 64 <= len {
            unsafe { masks64(ptr.add(o)) }
        } else {
            let r = len - o;
            tail[..r].copy_from_slice(&data[o..]);
            tail[r..].fill(0);
            unsafe { masks64(tail.as_ptr()) }
        };
        if o + 64 > len {
            let keep = (1u64 << (len - o)) - 1;
            c &= keep;
            n &= keep;
            q &= keep;
        }
        let mut seps = c | n;
        let mut qstart_mask = !0u64; // bits of this block that belong to the current line
        while seps != 0 {
            let p = seps.trailing_zeros() as usize;
            let bit = 1u64 << p;
            seps &= seps - 1;
            if c & bit != 0 {
                if nc < 8 {
                    commas[nc] = (o + p) as u32;
                }
                nc += 1;
            } else {
                let below = bit - 1;
                let quotes = qacc + (q & qstart_mask & below).count_ones();
                if let Some(f) = finish_line::<N>(data, line_start, o + p, &commas, nc, quotes) {
                    cb(f);
                }
                line_start = o + p + 1;
                nc = 0;
                qacc = 0;
                qstart_mask = !(below | bit);
            }
        }
        qacc += (q & qstart_mask).count_ones();
        o += 64;
    }
    if line_start < len {
        if let Some(f) = finish_line::<N>(data, line_start, len, &commas, nc, qacc) {
            cb(f);
        }
    }
}

#[inline(always)]
pub fn permute<const N: usize>(raw: [S; N], map: &[u8; 8]) -> [S; N] {
    let mut o: [S; N] = [&[]; N];
    for k in 0..N {
        o[k] = raw[map[k] as usize];
    }
    o
}

#[cfg(test)]
mod tests {
    use super::*;
    #[inline(always)]
pub fn old_parse_dt(b: &[u8]) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let y = d2(b, 0)? as i64 * 100 + d2(b, 2)? as i64;
    let mo = d2(b, 5)?;
    let d = d2(b, 8)?;
    let h = d2(b, 11)?;
    let mi = d2(b, 14)?;
    let s = d2(b, 17)?;
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || s > 59 || d < 1 {
        return None;
    }
    let dim = match mo {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
    };
    if d > dim {
        return None;
    }
    let days = days_from_civil(y, mo, d);
    Some(((days * 24 + h as i64) * 60 + mi as i64) * 60_000 + s as i64 * 1000)
}

    fn old_parse_ts(b: &[u8]) -> Option<i64> {
        match b.len() {
            20 if b[19] == b'Z' => old_parse_dt(&b[..19]),
            25 if b[19] == b'+' || b[19] == b'-' => {
                let base = old_parse_dt(&b[..19])?;
                if b[22] != b':' { return None; }
                let oh = d2(b, 20)?; let om = d2(b, 23)?;
                if oh > 23 || om > 59 { return None; }
                let off = (oh as i64 * 60 + om as i64) * 60_000;
                Some(if b[19] == b'+' { base - off } else { base + off })
            }
            _ => if all_digits(b) { parse_digits_i64(b) } else { None },
        }
    }
    struct R(u64);
    impl R { fn n(&mut self) -> u64 { self.0 ^= self.0 << 13; self.0 ^= self.0 >> 7; self.0 ^= self.0 << 17; self.0 } }
    #[test]
    fn ts_matches_old() {
        let mut r = R(0x9E3779B97F4A7C15);
        let alphabet: &[u8] = b"0123456789-T:Z+zt .\x7f";
        let mut n = 0u64;
        for _ in 0..3_000_000 {
            // well-formed base, then random mutations
            let y = r.n() % 10000; let mo = r.n() % 14; let d = r.n() % 33; let h = r.n() % 26; let mi = r.n() % 62; let s = r.n() % 62;
            let mut t = format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}", y, mo, d, h, mi, s).into_bytes();
            match r.n() % 3 { 0 => t.push(b'Z'), 1 => t.extend_from_slice(format!("{}{:02}:{:02}", if r.n() % 2 == 0 { '+' } else { '-' }, r.n() % 26, r.n() % 62).as_bytes()), _ => {} }
            let muts = r.n() % 3;
            for _ in 0..muts {
                let i = (r.n() as usize) % t.len();
                let c = alphabet[(r.n() as usize) % alphabet.len()];
                t[i] = if r.n() % 7 == 0 { 0xD9 } else { c };
            }
            assert_eq!(parse_ts(&t), old_parse_ts(&t), "{:?}", String::from_utf8_lossy(&t));
            if parse_ts(&t).is_some() { n += 1; }
        }
        // leap days and extremes
        for y in 0..10000 { for (mo, d) in [(2, 28), (2, 29), (2, 30), (12, 31), (1, 1)] {
            let t = format!("{:04}-{:02}-{:02}T23:59:59Z", y, mo, d);
            assert_eq!(parse_ts(t.as_bytes()), old_parse_ts(t.as_bytes()), "{t}");
        } }
        assert!(n > 100_000);
        eprintln!("valid {n}");
    }
}

#[cfg(test)]
mod f3_tests {
    use super::*;
    fn reference(b: &[u8]) -> Option<i64> {
        if all_digits(b) { parse_digits_i64(b) } else { None }
    }
    #[test]
    fn f3_matches_reference() {
        let mut x: u64 = 0x1234_5678_9ABC_DEF1;
        let mut next = || { x ^= x << 13; x ^= x >> 7; x ^= x << 17; x };
        let alphabet = b"0123456789012345678901234567890123456789.-e+ Z\xd9";
        for _ in 0..2_000_000 {
            let n = (next() % 22) as usize;
            let s: Vec<u8> = (0..n).map(|_| alphabet[(next() % alphabet.len() as u64) as usize]).collect();
            assert_eq!(parse_f3(&s), reference(&s), "{:?}", String::from_utf8_lossy(&s));
        }
        for s in ["1791071890000", "0001791071890000", "9999999999999999", "0", "86400000", "00000000000000000001791071890000"] {
            assert_eq!(parse_f3(s.as_bytes()), reference(s.as_bytes()), "{s}");
        }
    }
}
