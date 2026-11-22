// A 16-byte string handle: up to 15 bytes stored inline, longer strings by pointer.
// Inline strings compare with two integer compares and never touch the CSV memory.

use crate::S;

#[derive(Clone, Copy)]
#[repr(C, align(8))]
pub struct Small {
    b: [u8; 16],
}

const LONG: u8 = 0xFF;

impl Small {
    pub const EMPTY: Small = Small { b: [0; 16] };
    #[inline(always)]
    pub fn new(s: S) -> Small {
        let mut b = [0u8; 16];
        if s.len() <= 15 {
            b[..s.len()].copy_from_slice(s);
            b[15] = s.len() as u8;
        } else {
            b[..8].copy_from_slice(&(s.as_ptr() as u64).to_le_bytes());
            b[8..12].copy_from_slice(&(s.len() as u32).to_le_bytes());
            b[15] = LONG;
        }
        Small { b }
    }
    #[inline(always)]
    fn words(&self) -> (u64, u64) {
        (u64::from_le_bytes(self.b[..8].try_into().unwrap()), u64::from_le_bytes(self.b[8..].try_into().unwrap()))
    }
    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        if self.b[15] != LONG {
            &self.b[..self.b[15] as usize]
        } else {
            let (p, l) = self.words();
            unsafe { std::slice::from_raw_parts(p as *const u8, (l & 0xFFFF_FFFF) as usize) }
        }
    }
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.b[15] == 0
    }
}

impl PartialEq for Small {
    #[inline(always)]
    fn eq(&self, o: &Small) -> bool {
        let (a0, a1) = self.words();
        let (b0, b1) = o.words();
        if a0 == b0 && a1 == b1 {
            return true;
        }
        // differing words: equal only when both are long and their bytes match
        self.b[15] == LONG && o.b[15] == LONG && self.as_bytes() == o.as_bytes()
    }
}
impl Eq for Small {}

impl PartialOrd for Small {
    fn partial_cmp(&self, o: &Small) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Small {
    #[inline(always)]
    fn cmp(&self, o: &Small) -> std::cmp::Ordering {
        if self == o {
            std::cmp::Ordering::Equal
        } else {
            self.as_bytes().cmp(o.as_bytes())
        }
    }
}
