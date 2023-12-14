use crate::{small_bytes::SmallBytes, tid::Tid};
use std::{
    fs::File,
    io::{BufReader, Read},
    path::Path,
};

pub struct LogReader {
    file: BufReader<File>,
}

impl LogReader {
    pub fn new(path: &Path) -> std::io::Result<Self> {
        let file = BufReader::new(File::open(path)?);
        Ok(Self { file })
    }

    fn read_txn(&mut self) -> std::io::Result<LogTransaction> {
        let tid = Tid(self.file.read_u64()?);
        let num_records = self.file.read_u64()? as usize;
        let mut entries = Vec::with_capacity(num_records);
        for _ in 0..num_records {
            let key_len = self.file.read_u64()?;
            let key = self.file.read_exact_to_vec(key_len as usize)?;
            let value_len = self.file.read_u64()?;
            let value = if value_len == u64::MAX {
                None
            } else {
                Some(self.file.read_exact_to_vec(value_len as usize)?.into())
            };
            entries.push(LogEntry {
                key: key.into(),
                value,
            });
        }
        Ok(LogTransaction { tid, entries })
    }
}

impl Iterator for LogReader {
    type Item = std::io::Result<LogTransaction>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_txn() {
            Ok(txn) => Some(Ok(txn)),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // TODO: optionally give up recovery if logs are corrupted
                None
            }
            Err(e) => Some(Err(e)),
        }
    }
}

pub struct LogEntry {
    pub key: SmallBytes,
    pub value: Option<Box<[u8]>>,
}

pub struct LogTransaction {
    pub tid: Tid,
    pub entries: Vec<LogEntry>,
}

trait ReadBytesExt: Read {
    fn read_u64(&mut self) -> std::io::Result<u64> {
        let mut buf = [0; std::mem::size_of::<u64>()];
        self.read_exact(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }

    fn read_exact_to_vec(&mut self, len: usize) -> std::io::Result<Vec<u8>> {
        let mut buf = vec![0; len];
        self.read_exact(&mut buf)?;
        Ok(buf)
    }
}

impl<R: Read> ReadBytesExt for R {}
