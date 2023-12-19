use std::io::{Read, Write};

pub trait ReadBytesExt: Read {
    fn read_u64(&mut self) -> std::io::Result<u64> {
        let mut buf = [0; std::mem::size_of::<u64>()];
        self.read_exact(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }

    fn read_bytes(&mut self) -> std::io::Result<Vec<u8>> {
        let len = self.read_u64()?;
        self.read_exact_to_vec(len as usize)
    }

    fn read_maybe_bytes(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        let len = self.read_u64()?;
        // We can never hold u64::MAX bytes in memory.
        // So we use u64::MAX to represent None.
        Ok(if len == u64::MAX {
            None
        } else {
            Some(self.read_exact_to_vec(len as usize)?)
        })
    }

    fn read_exact_to_vec(&mut self, len: usize) -> std::io::Result<Vec<u8>> {
        let mut buf = vec![0; len];
        self.read_exact(&mut buf)?;
        Ok(buf)
    }
}

impl<R: Read> ReadBytesExt for R {}

pub trait WriteBytesExt: Write {
    fn write_u64(&mut self, n: u64) -> std::io::Result<()> {
        self.write_all(&n.to_le_bytes())
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.write_u64(bytes.len() as u64)?;
        self.write_all(bytes)
    }
}

impl<W: Write> WriteBytesExt for W {}

pub trait BytesExt {
    fn set_u64(&mut self, offset: usize, n: u64);
}

impl<T: AsMut<[u8]>> BytesExt for T {
    fn set_u64(&mut self, offset: usize, n: u64) {
        self.as_mut()[offset..][..std::mem::size_of::<u64>()].copy_from_slice(&n.to_le_bytes());
    }
}

pub trait ByteVecExt {
    fn write_u64(&mut self, n: u64);
    fn write_bytes(&mut self, bytes: &[u8]);
    fn write_maybe_bytes(&mut self, bytes: Option<&[u8]>);
}

impl ByteVecExt for Vec<u8> {
    fn write_u64(&mut self, n: u64) {
        self.extend_from_slice(&n.to_le_bytes());
    }

    fn write_bytes(&mut self, bytes: &[u8]) {
        ByteVecExt::write_u64(self, bytes.len() as u64);
        self.extend_from_slice(bytes);
    }

    fn write_maybe_bytes(&mut self, bytes: Option<&[u8]>) {
        match bytes {
            Some(bytes) => {
                ByteVecExt::write_u64(self, bytes.len() as u64);
                self.extend_from_slice(bytes);
            }
            None => ByteVecExt::write_u64(self, u64::MAX),
        }
    }
}
