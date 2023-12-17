use crate::{bytes_ext::ReadBytesExt, small_bytes::SmallBytes, tid::Tid};
use std::{fs::File, io::BufReader, path::Path};

pub struct LogReader {
    file: BufReader<File>,
}

impl LogReader {
    pub fn new(path: &Path) -> std::io::Result<Self> {
        Ok(Self {
            file: BufReader::new(File::open(path)?),
        })
    }

    fn read_txn(&mut self) -> std::io::Result<LogTransaction> {
        let tid = Tid(self.file.read_u64()?);
        let num_records = self.file.read_u64()? as usize;
        let mut entries = Vec::with_capacity(num_records);
        for _ in 0..num_records {
            let key = self.file.read_bytes()?;
            let value = self.file.read_maybe_bytes()?.map(Into::into);
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
