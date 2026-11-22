// A 16-byte string handle that compares and hashes without touching the CSV.
//
// Short (len <= 15): bytes inline, b[15] = len (top nibble 0).
// Long:  b[0..8]  = 64-bit xxh3 of the bytes,
//        b[8..14] = 48-bit pointer to the bytes,
//        b[14..16] = 0xF000 | len (len < 4096).
// Two long handles are equal when hash and length match, so equality, ordering
// and hashing are a few integer ops for every ID.

use crate::S;

#[derive(Clone, Copy)]
#[repr(C, align(8))]
pub struct Small {
    b: [u8; 16],
}

const PTR_MASK: u64 = (1 << 48) - 1;

#[inline(always)]
fn mum(a: u64, b: u64) -> u64 {
    let m = (a as u128) * (b as u128);
    (m as u64) ^ ((m >> 64) as u64)
}

impl Small {
    pub const EMPTY: Small = Small { b: [0; 16] };

    #[inline(always)]
    pub fn new(s: S) -> Small {
        if s.len() <= 15 {
            let mut b = [0u8; 16];
            b[..s.len()].copy_from_slice(s);
            b[15] = s.len() as u8;
            Small { b }
        } else {
            Small::long(s, xxhash_rust::xxh3::xxh3_64(s))
        }
    }

    /// Like `new`, but a short string is read with one 16-byte load when the 16
    /// bytes from its start lie before `end` (the end of readable memory).
    #[inline(always)]
    pub fn new_in(s: S, end: *const u8) -> Small {
        let n = s.len();
        if n <= 15 && (s.as_ptr() as usize) + 16 <= end as usize {
            // SAFETY: the 16 bytes at s.as_ptr() are inside the mapping (checked above)
            let raw = unsafe { std::ptr::read_unaligned(s.as_ptr() as *const u128) };
            let v = (raw & ((1u128 << (8 * n)) - 1)) | ((n as u128) << 120);
            return Small { b: v.to_le_bytes() };
        }
        Small::new(s)
    }

    #[inline(always)]
    fn long(s: S, h: u64) -> Small {
        let len = s.len().min(0x0FFF) as u64;
        let w1 = (s.as_ptr() as u64 & PTR_MASK) | ((0xF000 | len) << 48);
        let mut b = [0u8; 16];
        b[..8].copy_from_slice(&h.to_le_bytes());
        b[8..].copy_from_slice(&w1.to_le_bytes());
        Small { b }
    }

    #[inline(always)]
    fn words(&self) -> (u64, u64) {
        (u64::from_le_bytes(self.b[..8].try_into().unwrap()), u64::from_le_bytes(self.b[8..].try_into().unwrap()))
    }

    #[inline(always)]
    fn is_long(w1: u64) -> bool {
        w1 >> 60 == 0xF
    }

    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        let (_, w1) = self.words();
        if !Self::is_long(w1) {
            &self.b[..self.b[15] as usize]
        } else {
            let p = w1 & PTR_MASK;
            let l = ((w1 >> 48) & 0x0FFF) as usize;
            unsafe { std::slice::from_raw_parts(p as *const u8, l) }
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.b[15] == 0
    }

    /// A second 32-bit hash, independent of `h`, so (h, h2) identifies an ID
    /// with 96 bits. Long IDs read their bytes (call it while they are hot).
    #[inline(always)]
    pub fn h2(&self) -> u32 {
        let (w0, w1) = self.words();
        if Self::is_long(w1) {
            (xxhash_rust::xxh3::xxh3_64_with_seed(self.as_bytes(), 0x2545_F491_4F6C_DD1D) >> 32) as u32
        } else {
            mum(w0 ^ 0x9E37_79B9_7F4A_7C15, w1 ^ 0xD6E8_FEB8_6659_FD93) as u32
        }
    }

    /// 64-bit hash of the ID's bytes; the same for every handle of equal bytes.
    #[inline(always)]
    pub fn h(&self) -> u64 {
        let (w0, w1) = self.words();
        if Self::is_long(w1) {
            w0
        } else {
            mum(mum(w0 ^ 0xA076_1D64_78BD_642F, w1 ^ 0xE703_7ED1_A0B4_28DB) ^ 0x8EBC_6AF0_9C88_C6E3, 0x5899_65CC_7537_4CC3)
        }
    }

    /// Identity key: equal for equal IDs, a consistent total order otherwise.
    #[inline(always)]
    fn okey(&self) -> (u64, u64) {
        let (w0, w1) = self.words();
        if Self::is_long(w1) {
            (w1 >> 48, w0)
        } else {
            (w1, w0)
        }
    }
}

impl PartialEq for Small {
    #[inline(always)]
    fn eq(&self, o: &Small) -> bool {
        let (a0, a1) = self.words();
        let (b0, b1) = o.words();
        if a1 == b1 {
            return a0 == b0;
        }
        Self::is_long(a1) && a0 == b0 && (a1 >> 48) == (b1 >> 48)
    }
}
impl Eq for Small {}

impl PartialOrd for Small {
    fn partial_cmp(&self, o: &Small) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
/// Not byte order: only a consistent total order for grouping. Byte order,
/// where the rules need it, uses `as_bytes()`.
impl Ord for Small {
    #[inline(always)]
    fn cmp(&self, o: &Small) -> std::cmp::Ordering {
        self.okey().cmp(&o.okey())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn leak(s: &str) -> S {
        Box::leak(s.as_bytes().to_vec().into_boxed_slice())
    }
    #[test]
    fn long_and_short_identity() {
        let a = Small::new(leak("u_0123456789abcdef0123456789abcdef"));
        let b = Small::new(leak("u_0123456789abcdef0123456789abcdef"));
        let c = Small::new(leak("u_0123456789abcdef0123456789abcdee"));
        assert!(a == b && a.h() == b.h() && a.cmp(&b).is_eq());
        assert!(a != c && a.cmp(&c).is_ne());
        assert_eq!(a.as_bytes(), b"u_0123456789abcdef0123456789abcdef");
        let s = Small::new(leak("u1"));
        let t = Small::new(leak("u1"));
        let u = Small::new(leak("U1"));
        assert!(s == t && s.h() == t.h() && s != u && s != a && a != s);
        assert_eq!(s.as_bytes(), b"u1");
        assert!(Small::new(b"").is_empty() && !s.is_empty() && !a.is_empty());
        let x = Small::new(leak("abcdefghijklmno")); // 15: inline
        let y = Small::new(leak("abcdefghijklmnop")); // 16: long
        assert_eq!(x.as_bytes(), b"abcdefghijklmno");
        assert_eq!(y.as_bytes(), b"abcdefghijklmnop");
        assert!(x != y);
        // new_in matches new for every length, inside and at the edge of a buffer
        let buf = leak("abcdefghijklmnopqrstuvwxyz0123456789,\"x\",tail");
        let end = buf.as_ptr_range().end;
        for st in 0..buf.len() {
            for en in st..=buf.len() {
                let s = &buf[st..en];
                let (p, q) = (Small::new(s), Small::new_in(s, end));
                assert!(p == q && p.h() == q.h() && p.as_bytes() == q.as_bytes() && q.as_bytes() == s);
            }
        }
    }
}
