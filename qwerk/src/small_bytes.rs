#[cfg(target_pointer_width = "64")]
mod repr;

#[cfg(not(target_pointer_width = "64"))]
mod repr {
    #[derive(Clone)]
    pub struct Repr(Box<[u8]>);

    impl Repr {
        pub fn new(bytes: &[u8]) -> Self {
            Self(bytes.to_vec().into_boxed_slice())
        }

        pub const fn as_slice(&self) -> &[u8] {
            &self.0
        }
    }
}

use repr::Repr;

crate::assert_eq_size!(Repr, Box<[u8]>);
crate::assert_eq_size!(Repr, Option<Repr>);

#[derive(Clone)]
pub struct SmallBytes(Repr);

impl SmallBytes {
    pub fn new<T: AsRef<[u8]>>(bytes: T) -> Self {
        bytes.as_ref().into()
    }

    pub const fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl From<&[u8]> for SmallBytes {
    fn from(bytes: &[u8]) -> Self {
        Self(Repr::new(bytes))
    }
}

impl std::ops::Deref for SmallBytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl PartialEq for SmallBytes {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for SmallBytes {}

impl PartialOrd for SmallBytes {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SmallBytes {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}

impl std::hash::Hash for SmallBytes {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::SmallBytes;

    #[test]
    fn test() {
        let inline1 = SmallBytes::new(b"");
        assert!(inline1.as_slice().is_empty());

        let inline2 = SmallBytes::new(b"foo");
        assert_eq!(inline2.as_slice(), b"foo");

        let inline3 = SmallBytes::new(b"123456789012345");
        assert_eq!(inline3.as_slice(), b"123456789012345");

        let heap1 = SmallBytes::new(b"1234567890123456");
        assert_eq!(heap1.as_slice(), b"1234567890123456");

        let heap2 = SmallBytes::new(b"supercalifragilisticexpialidocious");
        assert_eq!(heap2.as_slice(), b"supercalifragilisticexpialidocious");
    }
}
