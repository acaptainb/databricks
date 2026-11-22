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

const _: () = assert!(std::mem::size_of::<Sv>() == 32);

struct SendPtr<T>(*mut T);
unsafe impl<T> Send for SendPtr<T> {}
unsafe impl<T> Sync for SendPtr<T> {}

fn build_links(files: &[S]) -> (LinkTable, usize) {
    let chunks = make_chunks_sized::<2>(files, &["user_id_a", "user_id_b"], 1 << 19);
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
    // name = byte-wise smallest ID of the component (parallel atomic min)
    vlog("links: roots done");
    use std::sync::atomic::{AtomicU32, Ordering as AO};
    let all: Vec<Small> = parts.iter().flat_map(|p| p.keys.iter().copied()).collect();
    let best: Vec<AtomicU32> = (0..n).map(|_| AtomicU32::new(u32::MAX)).collect();
    (0..n).into_par_iter().with_min_len(4096).for_each(|g| {
        let r = root[g] as usize;
        let kb = all[g].as_bytes();
        let mut cur = best[r].load(AO::Relaxed);
        loop {
            if cur != u32::MAX && all[cur as usize].as_bytes() <= kb {
                break;
            }
            match best[r].compare_exchange_weak(cur, g as u32, AO::Relaxed, AO::Relaxed) {
                Ok(_) => break,
                Err(c) => cur = c,
            }
        }
    });
    parts.par_iter_mut().enumerate().for_each(|(p, part)| {
        let b = pbase[p] as usize;
        let m = part.keys.len();
        part.name = (0..m).map(|i| all[best[root[b + i] as usize].load(AO::Relaxed) as usize]).collect();
    });
    drop(root);
    drop(best);
    drop(all);
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
    sub_bits: u32,
    sub_mask: usize,
}

/// The table is PP independent open-addressing sub-tables, one per ID hash
/// partition (the same partitions build_links interns into), so every
/// sub-table is built in parallel. A lookup is still one cache line.
impl LinkTable {
    fn build(parts: &[IdPart], _n: usize) -> LinkTable {
        let maxk = parts.iter().map(|p| p.keys.len()).max().unwrap_or(0);
        let sub = (maxk * 2).next_power_of_two().max(64);
        let cap = sub * PP;
        let bytes = cap * std::mem::size_of::<LSlot>();
        let m = memmap2::MmapMut::map_anon(bytes).expect("anon map");
        let _ = m.advise(memmap2::Advice::HugePage);
        let m: &'static mut memmap2::MmapMut = Box::leak(Box::new(m));
        // SAFETY: zeroed memory is a valid array of empty slots (key length 0)
        let slots: &'static mut [LSlot] = unsafe { std::slice::from_raw_parts_mut(m.as_mut_ptr() as *mut LSlot, cap) };
        let sub_mask = sub - 1;
        slots.par_chunks_mut(sub).zip(parts.par_iter()).for_each(|(st, part)| {
            for i in 0..part.keys.len() {
                let mut idx = (part.hashes[i] as usize) & sub_mask;
                while !st[idx].key.is_empty() {
                    idx = (idx + 1) & sub_mask;
                }
                st[idx] = LSlot { key: part.keys[i], name: part.name[i] };
            }
        });
        LinkTable { slots, sub_bits: sub.trailing_zeros(), sub_mask }
    }
    #[inline(always)]
    fn slot_of(&self, h: u64) -> usize {
        (upart(h) << self.sub_bits) | ((h as usize) & self.sub_mask)
    }
    #[inline(always)]
    fn prefetch(&self, h: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::*;
            _mm_prefetch(self.slots.as_ptr().add(self.slot_of(h)) as *const i8, _MM_HINT_T0);
        }
    }
    /// Person (name, name hash) of a user ID; an unlinked ID is its own person.
    #[inline(always)]
    fn person(&self, user: Small, h: u64) -> (Small, u64) {
        let base = upart(h) << self.sub_bits;
        let mut idx = (h as usize) & self.sub_mask;
        loop {
            let s = &self.slots[base + idx];
            if s.key.is_empty() {
                return (user, h);
            }
            if s.key == user {
                return (s.name, s.name.h());
            }
            idx = (idx + 1) & self.sub_mask;
        }
    }
}
// ---------------------------------------------------------------- records

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
const SV_PAD: Sv = Sv { k: 0, person: Small::EMPTY, price: PRICE_PAD, cat: 0 };

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
    fn finish(&mut self, out: &mut [Vec<SvPair>]) {
        for dst in 0..self.has.len() {
            if self.has[dst] {
                out[dst].push(SvPair([self.pending[dst], SV_PAD]));
                self.has[dst] = false;
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

// ---------------------------------------------------------------- fused event pass
//
// One pass over the event CSV. A valid copy is resolved to its person right away
// and written once, as a 32-byte session record, into that person's partition.
// Every copy (valid or not) also writes a 24-byte dedup key to its event_id
// bucket. Dedup then runs over the keys alone and returns the locations of the
// losing copies, which the session pass skips.

/// Location of a session record: thread slot << 56 | partition << 32 | index.
const LOC_INVALID: u64 = u64::MAX; // the copy failed validation: no record
const LOC_PAD: u64 = u64::MAX - 1; // block padding, not a copy

/// Session-record price sentinels (real prices are at most MAX_PRICE).
const PRICE_DEAD: u32 = u32::MAX - 1; // killed by dedup
const PRICE_PAD: u32 = u32::MAX; // pair padding (SV_PAD)

#[derive(Clone, Copy)]
#[repr(C)]
struct DKey {
    h: u64,
    h2: u32,
    ingest: u32, // epoch seconds (ingest_ts is whole seconds)
    loc: u64,
}
const DKEY_PAD: DKey = DKey { h: 0, h2: 0, ingest: u32::MAX, loc: LOC_PAD };

/// Eight keys: three full cache lines, written with non-temporal stores.
#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct DBlock([DKey; 8]);
const _: () = assert!(std::mem::size_of::<DBlock>() == 192);

/// A valid copy waiting for its person lookup (batched so lookups overlap).
#[derive(Clone, Copy)]
struct Pend {
    uh: u64,
    user: Small,
    ts: i64,
    h: u64,
    h2: u32,
    ingest: u32,
    price: u32,
    cat: u16,
}

const PEND_BATCH: usize = 16;

struct EvAcc {
    t: u64,
    sv: Vec<Vec<SvPair>>, // [partition]
    sv_cnt: Vec<u32>,
    sc: PairScatter,
    dk: Vec<Vec<DBlock>>, // [dedup bucket]
    st: Vec<DBlock>,
    st_n: Vec<u8>,
    pend: Vec<Pend>,
}

impl EvAcc {
    fn new(t: usize, sv_cap: usize, dk_cap: usize) -> EvAcc {
        EvAcc {
            t: t as u64,
            sv: (0..PP).map(|_| Vec::with_capacity(sv_cap)).collect(),
            sv_cnt: vec![0; PP],
            sc: PairScatter::new(PP),
            dk: (0..DB).map(|_| Vec::with_capacity(dk_cap)).collect(),
            st: vec![DBlock([DKEY_PAD; 8]); DB],
            st_n: vec![0; DB],
            pend: Vec::with_capacity(PEND_BATCH),
        }
    }

    #[inline(always)]
    fn push_key(&mut self, k: DKey) {
        let b = (k.h >> (64 - DB_BITS)) as usize;
        let n = self.st_n[b] as usize;
        self.st[b].0[n] = k;
        if n < 7 {
            self.st_n[b] = (n + 1) as u8;
            return;
        }
        self.st_n[b] = 0;
        let v = &mut self.dk[b];
        if v.len() == v.capacity() {
            v.reserve(v.capacity().max(16));
        }
        #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
        unsafe {
            use std::arch::x86_64::*;
            let d = v.as_mut_ptr().add(v.len()) as *mut __m256i;
            let s = &self.st[b] as *const DBlock as *const __m256i;
            for q in 0..6 {
                _mm256_stream_si256(d.add(q), _mm256_load_si256(s.add(q)));
            }
            v.set_len(v.len() + 1);
        }
        #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
        v.push(self.st[b]);
    }

    #[inline(always)]
    fn flush_pend(&mut self, links: &LinkTable, base: i64) {
        for i in 0..self.pend.len() {
            let p = self.pend[i];
            let (person, ph) = links.person(p.user, p.uh);
            let part = upart(ph);
            let idx = self.sv_cnt[part];
            self.sv_cnt[part] = idx + 1;
            self.sc.push(&mut self.sv, part, Sv { k: key(ph, p.ts - base), person, price: p.price, cat: p.cat });
            self.push_key(DKey { h: p.h, h2: p.h2, ingest: p.ingest, loc: (self.t << 56) | ((part as u64) << 32) | idx as u64 });
        }
        self.pend.clear();
    }

    /// Flush every staging buffer; call once after the last chunk.
    fn finish(&mut self, links: &LinkTable, base: i64) {
        self.flush_pend(links, base);
        self.sc.finish(&mut self.sv);
        for b in 0..DB {
            let n = self.st_n[b] as usize;
            if n > 0 {
                let mut blk = self.st[b];
                for k in &mut blk.0[n..] {
                    *k = DKEY_PAD;
                }
                self.dk[b].push(blk);
                self.st_n[b] = 0;
            }
        }
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_sfence();
        }
    }
}

fn parse_event_chunk(acc: &mut EvAcc, c: &Chunk, w: &Win, prods: &Products, links: &LinkTable, base: i64) {
    let end = c.data.as_ptr_range().end;
    scan::<7>(c.data, |raw| {
        let [eid, user, ts, ingest, et, prod, price] = permute(raw, &c.map);
        if eid.is_empty() {
            return;
        }
        let se = Small::new_in(eid, end);
        let h = se.h();
        let h2 = se.h2();
        let ingest_ms = parse_ts(ingest);
        // ingest_ts is always valid F1 (whole seconds); a bad one loses every tie
        let ing = match ingest_ms {
            Some(v) => v.div_euclid(1000).clamp(0, u32::MAX as i64 - 2) as u32,
            None => u32::MAX - 1,
        };
        if let (Some(ingv), Some(t)) = (ingest_ms, parse_ts(ts)) {
            if !user.is_empty() && t >= w.ws && t < w.we && !prod.is_empty() && ingv - t <= LATE {
                let k = event_kind(et);
                let mut ok = k != 0;
                let mut pr = 0u32;
                let mut cat = 0u16;
                if k == 3 {
                    match parse_price(price) {
                        Some(p) => {
                            pr = p;
                            cat = match prods.map.get(Small::new(prod)) {
                                Some(i) => prods.cat_of[i as usize],
                                None => prods.unknown,
                            };
                        }
                        None => ok = false,
                    }
                }
                if ok {
                    let usr = Small::new_in(user, end);
                    let uh = usr.h();
                    links.prefetch(uh);
                    acc.pend.push(Pend { uh, user: usr, ts: t, h, h2, ingest: ing, price: pr, cat });
                    if acc.pend.len() == PEND_BATCH {
                        acc.flush_pend(links, base);
                    }
                    return;
                }
            }
        }
        acc.push_key(DKey { h, h2, ingest: ing, loc: LOC_INVALID });
    });
}

/// Dedup one event_id bucket over its keys: per event keep the copy with the
/// smallest ingest_ts (the first seen on a tie: equal ingest means identical
/// copies). Returns the locations of the session records of losing copies.
fn dedup_keys(blocks: Vec<Vec<DBlock>>) -> Vec<u64> {
    let total: usize = blocks.iter().map(|v| v.len() * 8).sum();
    let mut table: HashTable<DKey> = HashTable::with_capacity(total);
    let mut kills = Vec::new();
    for v in &blocks {
        for blk in v {
            for k in &blk.0 {
                if k.loc == LOC_PAD {
                    continue;
                }
                // keys in a bucket share their top bits: move varying bits into the tag
                let th = k.h.rotate_left(32);
                match table.find_mut(th, |e| e.h == k.h && e.h2 == k.h2) {
                    Some(cur) => {
                        if k.ingest < cur.ingest {
                            if cur.loc != LOC_INVALID {
                                kills.push(cur.loc);
                            }
                            cur.ingest = k.ingest;
                            cur.loc = k.loc;
                        } else if k.loc != LOC_INVALID {
                            kills.push(k.loc);
                        }
                    }
                    None => {
                        table.insert_unique(th, *k, |e| e.h.rotate_left(32));
                    }
                }
            }
        }
    }
    kills
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
    // records with price PRICE_DEAD (killed by dedup) or PRICE_PAD (padding) are skipped
    let mut cnt = vec![0u32; NRB];
    for p in &parts {
        for pr in p {
            for e in &pr.0 {
                if e.price < PRICE_DEAD {
                    cnt[rbucket(e.k)] += 1;
                }
            }
        }
    }
    let n = starts_from_counts(&mut cnt);
    let starts = cnt.clone();
    let mut out: Vec<Sv> = Vec::with_capacity(n);
    let dst = out.as_mut_ptr();
    for p in &parts {
        for pr in p {
            for &e in &pr.0 {
                if e.price < PRICE_DEAD {
                    let b = rbucket(e.k);
                    // SAFETY: the counting pass sized every bucket; each slot is written once
                    unsafe { dst.add(cnt[b] as usize).write(e) };
                    cnt[b] += 1;
                }
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

/// Like per_thread, but the accumulator is told its thread slot.
fn per_thread_t<A: Send, C: Sync>(chunks: &[C], nthreads: usize, init: impl Fn(usize) -> A + Sync, f: impl Fn(&mut A, &C) + Sync) -> Vec<A> {
    use std::sync::Mutex;
    let slots: Vec<Mutex<Option<A>>> = (0..nthreads + 1).map(|_| Mutex::new(None)).collect();
    chunks.par_iter().for_each(|c| {
        let t = rayon::current_thread_index().unwrap_or(nthreads).min(nthreads);
        let mut g = slots[t].lock().unwrap();
        let acc = g.get_or_insert_with(|| init(t));
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
    assert!(nthreads < 255, "thread slot must fit 8 bits of a record location");
    let ev_bytes: usize = ev_chunks.iter().map(|c| c.data.len()).sum();
    let ck_bytes: usize = ck_chunks.iter().map(|c| c.data.len()).sum();
    let base = w.ws - WEEK;
    // expected records per thread per partition / bucket, with headroom
    let est_events = ev_bytes / 70;
    let sv_cap = est_events / nthreads / PP / 2 + 32;
    let dk_cap = est_events / nthreads / DB / 8 + 8;
    let ck_cap = ck_bytes / 40 / nthreads / PP + 32;

    // Phase 1: links, on all threads.
    let (links, nlink) = build_links(&ln_files);
    log(&format!("links: {nlink} ids"));

    // Phase 2: events and clicks in one parallel pass; both resolve persons.
    let (mut ev_accs, ck_jobs) = rayon::join(
        || {
            let r = per_thread_t(&ev_chunks, nthreads, |t| EvAcc::new(t, sv_cap, dk_cap), |acc, c| {
                parse_event_chunk(acc, c, &w, &prods, &links, base)
            });
            vlog("events: all chunks parsed");
            r
        },
        || {
            per_thread(&ck_chunks, nthreads, || (0..PP).map(|_| Vec::with_capacity(ck_cap)).collect::<Vec<Vec<Ck>>>(), |out, c| {
                parse_click_chunk(out, c, &w, &camps, &links)
            })
        },
    );
    ev_accs.par_iter_mut().for_each(|a| a.finish(&links, base));
    log(&format!("parsed events + clicks: {} chunks", ev_chunks.len()));

    // Phase 3: dedup over the keys; kills grouped by partition.
    // thread slot -> position in ev_accs
    let mut tpos = vec![usize::MAX; 256];
    for (i, a) in ev_accs.iter().enumerate() {
        tpos[a.t as usize] = i;
    }
    let mut sv_jobs: Vec<Vec<Vec<SvPair>>> = Vec::with_capacity(ev_accs.len());
    let mut dk_jobs: Vec<Vec<Vec<DBlock>>> = Vec::with_capacity(ev_accs.len());
    for a in ev_accs {
        sv_jobs.push(a.sv);
        dk_jobs.push(a.dk);
    }
    let dk_buckets = transpose(dk_jobs, DB);
    let kills: Vec<Vec<u64>> = dk_buckets.into_par_iter().map(dedup_keys).collect();
    let mut kills_pp: Vec<Vec<u64>> = (0..PP).map(|_| Vec::new()).collect();
    for kv in &kills {
        for &k in kv {
            kills_pp[((k >> 32) & 0xFFFF) as usize].push(k);
        }
    }
    drop(kills);
    log(&format!("dedup: {} copies dropped", kills_pp.iter().map(|v| v.len()).sum::<usize>()));

    // Phase 4: sessions.
    let ws_day = w.ws.div_euclid(DAY);
    let ndays = ((w.we - 1).div_euclid(DAY) - ws_day + 1).max(1) as usize;
    let cx = Ctx { base, ncamp: camps.channel.len(), ndays, ws_day, ncat: prods.cats.len() };
    let ev_by_pp = transpose(sv_jobs, PP);
    let ck_by_pp = transpose(ck_jobs, PP);
    let tpos = &tpos;
    let accs: Vec<Acc> = per_thread_owned(
        ev_by_pp.into_iter().zip(ck_by_pp).zip(kills_pp).collect(),
        nthreads,
        || Acc::new(&cx),
        |acc, ((mut ev_parts, ck_parts), kills): ((Vec<Vec<SvPair>>, Vec<Vec<Ck>>), Vec<u64>)| {
            for k in kills {
                let j = tpos[(k >> 56) as usize];
                let idx = (k & 0xFFFF_FFFF) as usize;
                ev_parts[j][idx / 2].0[idx % 2].price = PRICE_DEAD;
            }
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
