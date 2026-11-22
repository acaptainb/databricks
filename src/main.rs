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
    raw: Vec<NRef>, // the key's bytes in the links file
    hashes: Vec<u64>,
    name: Vec<NRef>,
    nh: Vec<u64>, // hash of the name
}

impl IdPart {
    fn new() -> Self {
        IdPart { table: HashTable::new(), keys: Vec::new(), raw: Vec::new(), hashes: Vec::new(), name: Vec::new(), nh: Vec::new() }
    }
    #[inline(always)]
    fn intern(&mut self, k: Small, h: u64, raw: NRef) -> u32 {
        let keys = &self.keys;
        if let Some(&i) = self.table.find(h, |&i| keys[i as usize] == k) {
            return i;
        }
        let i = self.keys.len() as u32;
        self.keys.push(k);
        self.raw.push(raw);
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

const _: () = assert!(std::mem::size_of::<Sv>() == 24);
const _: () = assert!(std::mem::size_of::<Ck>() == 24);

/// Reference to an ID's bytes in a mapped input file: 48-bit pointer, 16-bit length.
#[derive(Clone, Copy)]
pub struct NRef(u64);

impl NRef {
    const NONE: NRef = NRef(0);
    #[inline(always)]
    fn of(s: S) -> NRef {
        debug_assert!(s.len() <= 0xFFFF);
        NRef((s.as_ptr() as u64 & ((1 << 48) - 1)) | ((s.len().min(0xFFFF) as u64) << 48))
    }
    #[inline(always)]
    pub fn as_bytes(&self) -> &'static [u8] {
        let p = (self.0 & ((1 << 48) - 1)) as *const u8;
        let n = (self.0 >> 48) as usize;
        if n == 0 {
            return &[];
        }
        // SAFETY: built from a slice of a leaked (never unmapped) input mapping
        unsafe { std::slice::from_raw_parts(p, n) }
    }
}

struct SendPtr<T>(*mut T);
unsafe impl<T> Send for SendPtr<T> {}
unsafe impl<T> Sync for SendPtr<T> {}

fn build_links(files: &[S]) -> (LinkTable, usize) {
    let chunks = make_chunks_sized::<2>(files, &["user_id_a", "user_id_b"], 1 << 19);
    // parse, and scatter each side by hash partition: (id, hash, slot = pair*2+side)
    let parsed: Vec<(usize, Vec<Vec<(Small, u64, u32, NRef)>>)> = chunks
        .par_iter()
        .map(|c| {
            let mut v: Vec<Vec<(Small, u64, u32, NRef)>> = (0..PP).map(|_| Vec::new()).collect();
            let mut n = 0u32;
            scan::<2>(c.data, |raw| {
                let [a, b] = permute(raw, &c.map);
                if !a.is_empty() && !b.is_empty() {
                    let end = c.data.as_ptr_range().end;
                    let (sa, sb) = (Small::new_in(a, end), Small::new_in(b, end));
                    let ha = sa.h();
                    let hb_ = sb.h();
                    v[upart(ha)].push((sa, ha, n * 2, NRef::of(a)));
                    v[upart(hb_)].push((sb, hb_, n * 2 + 1, NRef::of(b)));
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
                for &(k, h, slot, raw) in &v[p] {
                    let i = part.intern(k, h, raw);
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
    let allr: Vec<NRef> = parts.iter().flat_map(|p| p.raw.iter().copied()).collect();
    let allh: Vec<u64> = parts.iter().flat_map(|p| p.hashes.iter().copied()).collect();
    parts.par_iter_mut().enumerate().for_each(|(p, part)| {
        let b = pbase[p] as usize;
        let m = part.keys.len();
        part.name = (0..m).map(|i| allr[best[root[b + i] as usize].load(AO::Relaxed) as usize]).collect();
        part.nh = (0..m).map(|i| allh[best[root[b + i] as usize].load(AO::Relaxed) as usize]).collect();
    });
    drop(root);
    drop(best);
    drop(all);
    drop(allr);
    drop(allh);
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
    name: NRef, // the person's name (smallest ID) in the links file
    nh: u64,    // hash of the name, as Small::h computes it
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
                let (name, nh) = (part.name[i], part.nh[i]);
                st[idx] = LSlot { key: part.keys[i], name, nh };
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
    /// Person (name, person hash) of a user ID; an unlinked ID is its own
    /// person, named by `uref` (its bytes) with hash `h`.
    #[inline(always)]
    fn person(&self, user: Small, h: u64, uref: NRef) -> (NRef, u64) {
        let base = upart(h) << self.sub_bits;
        let mut idx = (h as usize) & self.sub_mask;
        loop {
            let s = &self.slots[base + idx];
            if s.key.is_empty() {
                return (uref, h);
            }
            if s.key == user {
                return (s.name, s.nh);
            }
            idx = (idx + 1) & self.sub_mask;
        }
    }
}
// ---------------------------------------------------------------- records

/// Sort key: the low 33 bits of the 64-bit person hash, then 31 bits of time
/// (ms) relative to window_start - 7 days (the click window, at most 24 days).
/// Sorting by it groups persons and orders each person's records by time; the
/// high 32 bits of the person hash (`phi`) tell apart persons whose 33 bits
/// collide (fixed up after sorting).
const TS_BITS: u32 = 31;
const TS_MASK: u64 = (1 << TS_BITS) - 1;
const PHK_MASK: u64 = (1 << (64 - TS_BITS)) - 1;

#[inline(always)]
fn key(ph: u64, ts_rel: i64) -> u64 {
    ((ph & PHK_MASK) << TS_BITS) | (ts_rel as u64 & TS_MASK)
}

/// Price and category of a session record: price (1..=10,000,000, 24 bits) |
/// category << 24 for a purchase, 0 for a view or cart. Two values above every
/// real one mark records to skip.
const PC_DEAD: u32 = u32::MAX - 1; // killed by dedup
const PC_PAD: u32 = u32::MAX; // block padding

#[inline(always)]
fn pc_price(pc: u32) -> u64 {
    (pc & 0x00FF_FFFF) as u64
}
#[inline(always)]
fn pc_cat(pc: u32) -> usize {
    (pc >> 24) as usize
}

/// One surviving event in its person partition: 24 bytes.
#[derive(Clone, Copy)]
#[repr(C)]
struct Sv {
    k: u64,
    name: NRef, // the person's name bytes
    phi: u32,   // high 32 bits of the person hash
    pc: u32,
}
const SV_PAD: Sv = Sv { k: 0, name: NRef::NONE, phi: 0, pc: PC_PAD };

/// Eight session records: three full cache lines, written with non-temporal stores.
#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct SvBlock([Sv; 8]);
const _: () = assert!(std::mem::size_of::<SvBlock>() == 192);

/// Per-partition staging so every store to memory is a full cache line.
struct SvStage {
    st: Vec<SvBlock>,
    n: Vec<u8>,
}

impl SvStage {
    fn new(np: usize) -> Self {
        SvStage { st: vec![SvBlock([SV_PAD; 8]); np], n: vec![0; np] }
    }
    #[inline(always)]
    fn push(&mut self, out: &mut [Vec<SvBlock>], dst: usize, e: Sv) {
        let n = self.n[dst] as usize;
        self.st[dst].0[n] = e;
        if n < 7 {
            self.n[dst] = (n + 1) as u8;
            return;
        }
        self.n[dst] = 0;
        let v = &mut out[dst];
        if v.len() == v.capacity() {
            v.reserve(v.capacity().max(16));
        }
        #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
        unsafe {
            use std::arch::x86_64::*;
            let d = v.as_mut_ptr().add(v.len()) as *mut __m256i;
            let s = &self.st[dst] as *const SvBlock as *const __m256i;
            for q in 0..6 {
                _mm256_stream_si256(d.add(q), _mm256_load_si256(s.add(q)));
            }
            v.set_len(v.len() + 1);
        }
        #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
        v.push(self.st[dst]);
    }
    fn finish(&mut self, out: &mut [Vec<SvBlock>]) {
        for dst in 0..self.n.len() {
            let n = self.n[dst] as usize;
            if n > 0 {
                let mut blk = self.st[dst];
                for e in &mut blk.0[n..] {
                    *e = SV_PAD;
                }
                out[dst].push(blk);
                self.n[dst] = 0;
            }
        }
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_sfence();
        }
    }
}

/// One valid click in its person partition: 24 bytes.
#[derive(Clone, Copy)]
#[repr(C)]
struct Ck {
    k: u64,
    cid: NRef, // click_id bytes, for the tie-break
    phi: u32,
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
// and written once, as a 24-byte session record, into that person's partition.
// Every copy (valid or not) also writes a 16-byte dedup key to its event_id
// bucket. Dedup then runs over the keys alone and returns the locations of the
// losing copies, which the session pass skips.

/// Location of a session record (u32): thread slot | partition | index, with
/// the field widths of `LocLayout`. The two top values are reserved.
const LOC_INVALID: u32 = u32::MAX; // the copy failed validation: no record
const LOC_PAD: u32 = u32::MAX - 1; // block padding, not a copy

#[derive(Clone, Copy)]
struct LocLayout {
    tshift: u32, // thread slot << tshift
    ibits: u32,  // index bits (partition sits between)
}

impl LocLayout {
    /// Slots 0..=nthreads must leave the all-ones slot unused (reserved values).
    fn new(nthreads: usize) -> LocLayout {
        let mut tbits = 1;
        while (1usize << tbits) < nthreads + 2 {
            tbits += 1;
        }
        let ibits = 32 - tbits - PP_BITS;
        assert!(ibits >= 16, "too many threads for 32-bit record locations");
        LocLayout { tshift: 32 - tbits, ibits }
    }
    #[inline(always)]
    fn pack(&self, t: u32, part: usize, idx: u32) -> u32 {
        (t << self.tshift) | ((part as u32) << self.ibits) | idx
    }
    #[inline(always)]
    fn unpack(&self, loc: u32) -> (usize, usize, usize) {
        let t = (loc >> self.tshift) as usize;
        let part = ((loc >> self.ibits) as usize) & (PP - 1);
        let idx = (loc & ((1u32 << self.ibits) - 1)) as usize;
        (t, part, idx)
    }
}

/// Dedup key: 16 bytes. `hx` is the 64-bit event_id hash with its top DB_BITS
/// (implied by the bucket) replaced by bits of an independent second hash, so
/// a key identifies its event_id with 64 + DB_BITS bits.
#[derive(Clone, Copy)]
#[repr(C)]
struct DKey {
    hx: u64,
    ingest: u32, // epoch seconds (ingest_ts is whole seconds)
    loc: u32,
}
const DKEY_PAD: DKey = DKey { hx: 0, ingest: u32::MAX, loc: LOC_PAD };

#[inline(always)]
fn dkey_hx(h: u64, h2: u32) -> u64 {
    let low = (1u64 << (64 - DB_BITS)) - 1;
    (h & low) | (((h2 as u64) & ((1 << DB_BITS) - 1)) << (64 - DB_BITS))
}

/// Four keys: one cache line, written with a non-temporal store.
#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct DBlock([DKey; 4]);
const _: () = assert!(std::mem::size_of::<DBlock>() == 64);

/// A valid copy waiting for its person lookup (batched so lookups overlap).
#[derive(Clone, Copy)]
struct Pend {
    uh: u64,
    user: Small,
    uref: NRef,
    ts: i64,
    bucket: u32,
    hx: u64,
    ingest: u32,
    pc: u32,
}

const PEND_BATCH: usize = 16;

struct EvAcc {
    t: u32,
    lay: LocLayout,
    sv: Vec<Vec<SvBlock>>, // [partition]
    sv_cnt: Vec<u32>,
    sc: SvStage,
    dk: Vec<Vec<DBlock>>, // [dedup bucket]
    st: Vec<DBlock>,
    st_n: Vec<u8>,
    pend: Vec<Pend>,
    deferred: Vec<Pend>, // valid copies parsed before the link table was ready
}

impl EvAcc {
    fn new(t: usize, lay: LocLayout, sv_cap: usize, dk_cap: usize) -> EvAcc {
        EvAcc {
            t: t as u32,
            lay,
            sv: (0..PP).map(|_| Vec::with_capacity(sv_cap)).collect(),
            sv_cnt: vec![0; PP],
            sc: SvStage::new(PP),
            dk: (0..DB).map(|_| Vec::with_capacity(dk_cap)).collect(),
            st: vec![DBlock([DKEY_PAD; 4]); DB],
            st_n: vec![0; DB],
            pend: Vec::with_capacity(PEND_BATCH),
            deferred: Vec::new(),
        }
    }

    #[inline(always)]
    fn push_key(&mut self, b: usize, k: DKey) {
        let n = self.st_n[b] as usize;
        self.st[b].0[n] = k;
        if n < 3 {
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
            _mm256_stream_si256(d, _mm256_load_si256(s));
            _mm256_stream_si256(d.add(1), _mm256_load_si256(s.add(1)));
            v.set_len(v.len() + 1);
        }
        #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
        v.push(self.st[b]);
    }

    #[inline(always)]
    fn flush_pend(&mut self, links: &LinkTable, base: i64) {
        for i in 0..self.pend.len() {
            let p = self.pend[i];
            let (name, ph) = links.person(p.user, p.uh, p.uref);
            let part = upart(ph);
            let idx = self.sv_cnt[part];
            assert!(idx < (1u32 << self.lay.ibits), "record index overflows its location field");
            self.sv_cnt[part] = idx + 1;
            self.sc.push(&mut self.sv, part, Sv { k: key(ph, p.ts - base), name, phi: (ph >> 32) as u32, pc: p.pc });
            let loc = self.lay.pack(self.t, part, idx);
            self.push_key(p.bucket as usize, DKey { hx: p.hx, ingest: p.ingest, loc });
        }
        self.pend.clear();
    }

    /// Flush every staging buffer; call once after the last chunk.
    fn finish(&mut self, links: &LinkTable, base: i64) {
        self.flush_pend(links, base);
        // copies parsed before the link table existed: resolve them now, prefetched in batches
        let deferred = std::mem::take(&mut self.deferred);
        for chunk in deferred.chunks(PEND_BATCH) {
            for p in chunk {
                links.prefetch(p.uh);
            }
            self.pend.extend_from_slice(chunk);
            self.flush_pend(links, base);
        }
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

/// `links` is None while the link table is still being built: valid copies are
/// then kept in `acc.deferred` and resolved in `EvAcc::finish`.
fn parse_event_chunk(acc: &mut EvAcc, c: &Chunk, w: &Win, prods: &Products, links: Option<&LinkTable>, base: i64) {
    let end = c.data.as_ptr_range().end;
    scan::<7>(c.data, |raw| {
        let [eid, user, ts, ingest, et, prod, price] = permute(raw, &c.map);
        if eid.is_empty() {
            return;
        }
        let se = Small::new_in(eid, end);
        let h = se.h();
        let bucket = (h >> (64 - DB_BITS)) as u32;
        let hx = dkey_hx(h, se.h2());
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
                let mut pc = 0u32;
                if k == 3 {
                    match parse_price(price) {
                        Some(p) => {
                            let cat = match prods.map.get(Small::new(prod)) {
                                Some(i) => prods.cat_of[i as usize],
                                None => prods.unknown,
                            };
                            pc = p | ((cat as u32) << 24);
                        }
                        None => ok = false,
                    }
                }
                if ok {
                    let usr = Small::new_in(user, end);
                    let uh = usr.h();
                    let p = Pend { uh, user: usr, uref: NRef::of(user), ts: t, bucket, hx, ingest: ing, pc };
                    match links {
                        Some(l) => {
                            l.prefetch(uh);
                            acc.pend.push(p);
                            if acc.pend.len() == PEND_BATCH {
                                acc.flush_pend(l, base);
                            }
                        }
                        None => acc.deferred.push(p),
                    }
                    return;
                }
            }
        }
        acc.push_key(bucket as usize, DKey { hx, ingest: ing, loc: LOC_INVALID });
    });
}

/// Dedup one event_id bucket over its keys: per event keep the copy with the
/// smallest ingest_ts (the first seen on a tie: equal ingest means identical
/// copies). Returns the locations of the session records of losing copies.
fn dedup_keys(blocks: Vec<Vec<DBlock>>) -> Vec<u32> {
    let total: usize = blocks.iter().map(|v| v.len() * 4).sum();
    let mut table: HashTable<DKey> = HashTable::with_capacity(total);
    let mut kills = Vec::new();
    for v in &blocks {
        for blk in v {
            for k in &blk.0 {
                if k.loc == LOC_PAD {
                    continue;
                }
                match table.find_mut(k.hx, |e| e.hx == k.hx) {
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
                        table.insert_unique(k.hx, *k, |e| e.hx);
                    }
                }
            }
        }
    }
    kills
}

/// Per-thread click accumulator: clicks by person partition, plus valid clicks
/// parsed before the link table was ready (resolved by `CkAcc::finish`).
struct CkAcc {
    out: Vec<Vec<Ck>>,
    deferred: Vec<CkRaw>,
}

impl CkAcc {
    fn new(cap: usize) -> CkAcc {
        CkAcc { out: (0..PP).map(|_| Vec::with_capacity(cap)).collect(), deferred: Vec::new() }
    }
    #[inline(always)]
    fn emit(&mut self, c: &CkRaw, links: &LinkTable, lo: i64) {
        let (_, ph) = links.person(c.user, c.uh, NRef::NONE);
        self.out[upart(ph)].push(Ck { k: key(ph, c.ts - lo), cid: NRef::of(c.cid), phi: (ph >> 32) as u32, camp: c.camp });
    }
    fn finish(&mut self, links: &LinkTable, lo: i64) {
        let deferred = std::mem::take(&mut self.deferred);
        for chunk in deferred.chunks(16) {
            for c in chunk {
                links.prefetch(c.uh);
            }
            for c in chunk {
                self.emit(c, links, lo);
            }
        }
    }
}

fn parse_click_chunk(acc: &mut CkAcc, c: &Chunk, w: &Win, camps: &Campaigns, links: Option<&LinkTable>) {
    let end = c.data.as_ptr_range().end;
    const B: usize = 16;
    let lo = w.ws - WEEK;
    let mut batch: [Option<CkRaw>; B] = [None; B];
    let mut nb = 0usize;
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
        let r = CkRaw { uh, ts: t, user: su, cid, camp: ci as u16 };
        match links {
            Some(l) => {
                l.prefetch(uh);
                batch[nb] = Some(r);
                nb += 1;
                if nb == B {
                    for slot in batch.iter_mut() {
                        acc.emit(&slot.take().unwrap(), l, lo);
                    }
                    nb = 0;
                }
            }
            None => acc.deferred.push(r),
        }
    });
    if let Some(l) = links {
        for slot in batch.iter_mut().take(nb) {
            acc.emit(&slot.take().unwrap(), l, lo);
        }
    }
}

// ---------------------------------------------------------------- sessions and aggregation

pub struct UserRow {
    pub name: NRef,
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
fn fix_collisions<T>(v: &mut [T], ph: impl Fn(&T) -> u64, pb: impl Fn(&T) -> u32) {
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

/// Sort a large bucket by key. Every key in a bucket shares its top RB bits, so
/// an LSD radix sort over the low 64 - RB bits (14 per pass, skipping passes
/// where all keys share the digit, as for one whale person's hash bits) gives
/// the full key order.
fn sort_big_sv(v: &mut [Sv]) {
    let n = v.len();
    if n < 4096 {
        v.sort_unstable_by_key(|x| x.k);
        return;
    }
    const D: u32 = 14;
    const ND: usize = 1 << D;
    let mut tmp: Vec<Sv> = Vec::with_capacity(n);
    // SAFETY: every slot of tmp is written by a scatter pass before it is read
    unsafe { tmp.set_len(n) };
    let mut src: *mut Sv = v.as_mut_ptr();
    let mut dst: *mut Sv = tmp.as_mut_ptr();
    let mut cnt = vec![0usize; ND];
    let mut shift = 0u32;
    while shift < 64 - RB {
        cnt.iter_mut().for_each(|c| *c = 0);
        for i in 0..n {
            let k = unsafe { (*src.add(i)).k };
            cnt[((k >> shift) as usize) & (ND - 1)] += 1;
        }
        if cnt.iter().any(|&c| c == n) {
            shift += D;
            continue;
        }
        let mut s = 0usize;
        for c in cnt.iter_mut() {
            let x = *c;
            *c = s;
            s += x;
        }
        for i in 0..n {
            let e = unsafe { *src.add(i) };
            let d = ((e.k >> shift) as usize) & (ND - 1);
            unsafe { dst.add(cnt[d]).write(e) };
            cnt[d] += 1;
        }
        std::mem::swap(&mut src, &mut dst);
        shift += D;
    }
    if src != v.as_mut_ptr() {
        // SAFETY: src is tmp, fully written by the last pass
        unsafe { std::ptr::copy_nonoverlapping(src, v.as_mut_ptr(), n) };
    }
}
/// Concatenate the per-job event pairs, sorted by key.
fn sorted_events(parts: Vec<Vec<SvBlock>>) -> Vec<Sv> {
    // records marked PC_DEAD (killed by dedup) or PC_PAD (padding) are skipped
    let mut cnt = vec![0u32; NRB];
    for p in &parts {
        for pr in p {
            for e in &pr.0 {
                if e.pc < PC_DEAD {
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
                if e.pc < PC_DEAD {
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
        small_sort(&mut out[starts[b] as usize..e], |x, y| x.k < y.k, sort_big_sv);
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
        small_sort(&mut out[starts[b] as usize..e], |x, y| x.k < y.k || (x.k == y.k && x.cid.as_bytes() > y.cid.as_bytes()), |s| {
            s.sort_unstable_by(|x, y| x.k.cmp(&y.k).then_with(|| y.cid.as_bytes().cmp(x.cid.as_bytes())))
        });
    }
    out
}

fn process_partition(acc: &mut Acc, cx: &Ctx, evs: &mut [Sv], cks: &mut [Ck]) {
    // evs and cks arrive sorted by key (clicks: then click_id descending)
    fix_collisions(evs, |e| e.k >> TS_BITS, |e| e.phi);
    fix_collisions(cks, |c| c.k >> TS_BITS, |c| c.phi);

    let ndays = cx.ndays;
    let organic = cx.ncamp;
    let n = evs.len();
    let nc = cks.len();
    let mut cj = 0usize;
    let mut i = 0usize;
    while i < n {
        let ph = evs[i].k >> TS_BITS;
        let phi = evs[i].phi;
        let person = evs[i].name;
        let mut b = i + 1;
        while b < n && evs[b].k >> TS_BITS == ph && evs[b].phi == phi {
            b += 1;
        }
        while cj < nc && (cks[cj].k >> TS_BITS < ph || (cks[cj].k >> TS_BITS == ph && cks[cj].phi < phi)) {
            cj += 1;
        }
        let c0 = cj;
        let mut c1 = cj;
        while c1 < nc && cks[c1].k >> TS_BITS == ph && cks[c1].phi == phi {
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
                if e.pc != 0 {
                    let p = pc_price(e.pc);
                    rev += p;
                    acc.catsum[pc_cat(e.pc)] += p;
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
fn flush_user(acc: &mut Acc, cx: &Ctx, name: NRef, day: i32, sessions: u32, events: u64, revenue: u64) {
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

/// Consume owned work items, largest first, from a shared queue: one
/// accumulator per worker. Largest-first keeps a huge item (a whale person's
/// partition) from starting last and leaving the other threads idle.
fn per_thread_owned<A: Send, W: Send>(
    mut items: Vec<W>,
    nthreads: usize,
    size: impl Fn(&W) -> usize,
    init: impl Fn() -> A + Sync,
    f: impl Fn(&mut A, W) + Sync,
) -> Vec<A> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    items.sort_by_key(|w| std::cmp::Reverse(size(w)));
    let n = items.len();
    let cells: Vec<Mutex<Option<W>>> = items.into_iter().map(|w| Mutex::new(Some(w))).collect();
    let next = AtomicUsize::new(0);
    (0..nthreads.max(1))
        .into_par_iter()
        .with_max_len(1)
        .map(|_| {
            let mut acc = init();
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= n {
                    break;
                }
                let w = cells[i].lock().unwrap().take().unwrap();
                f(&mut acc, w);
            }
            acc
        })
        .collect()
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
    let base = w.ws - WEEK;
    assert!(w.we - base < (1i64 << TS_BITS), "window too long for 31-bit relative times");
    assert!(prods.cats.len() <= 256, "more categories than an 8-bit category field holds");
    let lay = LocLayout::new(nthreads);
    // expected blocks per thread per partition / bucket, with headroom
    let est_events = ev_bytes / 70;
    let sv_cap = est_events / nthreads / PP / 8 + 8;
    let dk_cap = est_events / nthreads / DB / 4 + 8;
    let ck_cap = ck_bytes / 40 / nthreads / PP + 32;

    // Phases 1 + 2 overlapped: links are built while events and clicks are
    // already being parsed; copies parsed before the link table is ready are
    // kept and resolved once it is.
    let links_cell: std::sync::OnceLock<(LinkTable, usize)> = std::sync::OnceLock::new();
    let ((), (mut ev_accs, mut ck_accs)) = rayon::join(
        || {
            let r = build_links(&ln_files);
            let _ = links_cell.set(r);
            vlog("links ready");
        },
        || {
            rayon::join(
                || {
                    let r = per_thread_t(&ev_chunks, nthreads, |t| EvAcc::new(t, lay, sv_cap, dk_cap), |acc, c| {
                        parse_event_chunk(acc, c, &w, &prods, links_cell.get().map(|x| &x.0), base)
                    });
                    vlog("events: all chunks parsed");
                    r
                },
                || {
                    per_thread(&ck_chunks, nthreads, || CkAcc::new(ck_cap), |acc, c| {
                        parse_click_chunk(acc, c, &w, &camps, links_cell.get().map(|x| &x.0))
                    })
                },
            )
        },
    );
    let (links, nlink) = links_cell.get().expect("link table");
    let nlink = *nlink;
    rayon::join(
        || ev_accs.par_iter_mut().for_each(|a| a.finish(links, base)),
        || ck_accs.par_iter_mut().for_each(|a| a.finish(links, w.ws - WEEK)),
    );
    let ck_jobs: Vec<Vec<Vec<Ck>>> = ck_accs.into_iter().map(|a| a.out).collect();
    log(&format!("links: {nlink} ids; parsed events + clicks: {} chunks", ev_chunks.len()));

    // Phase 3: dedup over the keys; kills grouped by partition.
    // thread slot -> position in ev_accs
    let mut tpos = vec![usize::MAX; 1 << (32 - lay.tshift)];
    for (i, a) in ev_accs.iter().enumerate() {
        tpos[a.t as usize] = i;
    }
    let mut sv_jobs: Vec<Vec<Vec<SvBlock>>> = Vec::with_capacity(ev_accs.len());
    let mut dk_jobs: Vec<Vec<Vec<DBlock>>> = Vec::with_capacity(ev_accs.len());
    for a in ev_accs {
        sv_jobs.push(a.sv);
        dk_jobs.push(a.dk);
    }
    let dk_buckets = transpose(dk_jobs, DB);
    let kills: Vec<Vec<u32>> = dk_buckets.into_par_iter().map(dedup_keys).collect();
    let mut kills_pp: Vec<Vec<u32>> = (0..PP).map(|_| Vec::new()).collect();
    for kv in &kills {
        for &k in kv {
            kills_pp[lay.unpack(k).1].push(k);
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
        |((ev, ck), _): &((Vec<Vec<SvBlock>>, Vec<Vec<Ck>>), Vec<u32>)| ev.iter().map(|v| v.len() * 8).sum::<usize>() + ck.iter().map(|v| v.len()).sum::<usize>(),
        || Acc::new(&cx),
        |acc, ((mut ev_parts, ck_parts), kills): ((Vec<Vec<SvBlock>>, Vec<Vec<Ck>>), Vec<u32>)| {
            for k in kills {
                let (t, _, idx) = lay.unpack(k);
                ev_parts[tpos[t]][idx / 8].0[idx % 8].pc = PC_DEAD;
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
    release_inputs();
    std::process::exit(0);
}
