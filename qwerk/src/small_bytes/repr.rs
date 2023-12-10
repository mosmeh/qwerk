static_assertions::assert_eq_size!(usize, u64);

const MAX_INLINE_LEN: usize = 15;

/// An owned, immutable sequence of bytes that can store up to 15 bytes inline.
///
/// Values larger than 15 bytes are stored on the heap.
#[repr(C)]
pub struct Repr(
    *mut u8, // pointer to the first byte
    u32,     // length bits 0..31
    u16,     // length bits 32..47
    u8,      // length bits 48..55
    Tag,
);

static_assertions::assert_eq_size!(Repr, [u8; MAX_INLINE_LEN + 1]);

unsafe impl Send for Repr {}
unsafe impl Sync for Repr {}

#[allow(dead_code)]
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Tag {
    // We list all the possible tags here so that the compiler can perform
    // niche-filling optimizations.
    Heap = 0,
    InlineLen0 = 1,
    InlineLen1 = 2,
    InlineLen2 = 3,
    InlineLen3 = 4,
    InlineLen4 = 5,
    InlineLen5 = 6,
    InlineLen6 = 7,
    InlineLen7 = 8,
    InlineLen8 = 9,
    InlineLen9 = 10,
    InlineLen10 = 11,
    InlineLen11 = 12,
    InlineLen12 = 13,
    InlineLen13 = 14,
    InlineLen14 = 15,
    InlineLen15 = 16,
}

impl Repr {
    pub fn from_slice(bytes: &[u8]) -> Self {
        let len = bytes.len();
        if len <= MAX_INLINE_LEN {
            unsafe { Self::new_inline_unchecked(bytes) }
        } else {
            Self::new_heap(bytes.to_vec().into_boxed_slice())
        }
    }

    pub fn from_boxed_slice(bytes: Box<[u8]>) -> Self {
        let len = bytes.len();
        if len <= MAX_INLINE_LEN {
            unsafe { Self::new_inline_unchecked(&bytes) }
        } else {
            Self::new_heap(bytes)
        }
    }

    unsafe fn new_inline_unchecked(bytes: &[u8]) -> Self {
        let mut buf = [0; MAX_INLINE_LEN + 1];
        buf[MAX_INLINE_LEN] = bytes.len() as u8 + Tag::InlineLen0 as u8;
        buf[..bytes.len()].copy_from_slice(bytes);
        std::mem::transmute(buf)
    }

    fn new_heap(bytes: Box<[u8]>) -> Self {
        let len = bytes.len();
        assert!(len < (1 << (32 + 16 + 8))); // 64 petabytes. Should be enough.
        let ptr = Box::into_raw(bytes);
        Self(
            ptr.cast(),
            len as u32,
            (len >> 32) as u16,
            (len >> 48) as u8,
            Tag::Heap,
        )
    }

    pub const fn as_slice(&self) -> &[u8] {
        let (ptr, len) = if let Some(len) = (self.tag() as u8).checked_sub(Tag::InlineLen0 as u8) {
            ((self as *const Self).cast::<u8>(), len as usize)
        } else {
            (self.heap_ptr().cast_const(), self.heap_len())
        };
        unsafe { std::slice::from_raw_parts(ptr, len) }
    }

    const fn tag(&self) -> Tag {
        self.4
    }

    const fn heap_ptr(&self) -> *mut u8 {
        self.0
    }

    const fn heap_len(&self) -> usize {
        self.1 as usize | ((self.2 as usize) << 32) | ((self.3 as usize) << 48)
    }
}

impl Drop for Repr {
    fn drop(&mut self) {
        if self.tag() == Tag::Heap {
            let ptr = self.heap_ptr();
            let len = self.heap_len();
            let _ = unsafe { Box::from_raw(std::slice::from_raw_parts_mut(ptr, len)) };
        }
    }
}

impl Clone for Repr {
    fn clone(&self) -> Self {
        Self::from_slice(self.as_slice())
    }
}
