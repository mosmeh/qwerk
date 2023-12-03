#[cfg(target_pointer_width = "64")]
mod repr;

#[cfg(not(target_pointer_width = "64"))]
mod repr {
    #[derive(Clone)]
    pub struct Repr(Box<[u8]>);

    impl Repr {
        pub fn from_slice(bytes: &[u8]) -> Self {
            Self(bytes.to_vec().into_boxed_slice())
        }

        pub fn from_boxed_slice(bytes: Box<[u8]>) -> Self {
            Self(bytes)
        }

        pub const fn as_slice(&self) -> &[u8] {
            &self.0
        }
    }
}

use repr::Repr;

crate::assert_eq_size!(Repr, Box<[u8]>);
crate::assert_eq_size!(Repr, Option<Repr>);

/// An owned, immutable sequence of bytes.
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
        Self(Repr::from_slice(bytes))
    }
}

impl From<Vec<u8>> for SmallBytes {
    fn from(bytes: Vec<u8>) -> Self {
        bytes.into_boxed_slice().into()
    }
}

impl From<Box<[u8]>> for SmallBytes {
    fn from(bytes: Box<[u8]>) -> Self {
        Self(Repr::from_boxed_slice(bytes))
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
        for bytes in [
            b"".as_slice(),
            b"foo",
            b"123456789012345",
            b"1234567890123456",
            b"supercalifragilisticexpialidocious",
        ] {
            let from_slice = SmallBytes::new(bytes);
            assert_eq!(from_slice.as_slice(), bytes);

            let from_vec = SmallBytes::from(bytes.to_vec());
            assert_eq!(from_vec.as_slice(), bytes);
        }
    }
}
