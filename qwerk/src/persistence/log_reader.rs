use crate::{bytes_ext::ReadBytesExt, small_bytes::SmallBytes, tid::Tid, Error, Result};
use std::{
    cmp::Ordering,
    fs::File,
    io::{BufReader, Read},
    path::Path,
};

pub struct LogReader {
    file: ReadBytesCounter<BufReader<File>>,
    file_len: u64,
}

impl LogReader {
    pub fn new(path: &Path) -> std::io::Result<Self> {
        let file = File::open(path)?;
        let file_len = file.metadata()?.len();
        Ok(Self {
            file: ReadBytesCounter::new(BufReader::new(file)),
            file_len,
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
    type Item = Result<LogTransaction>;

    fn next(&mut self) -> Option<Self::Item> {
        let num_bytes_read = self.file.num_bytes_read() as u64;
        match num_bytes_read.cmp(&self.file_len) {
            Ordering::Less => Some(self.read_txn().map_err(Into::into)),
            Ordering::Equal => {
                // Make sure we have reached EOF.
                match self.file.read(&mut [0; 1]) {
                    Ok(0) => None,
                    Ok(_) => Some(Err(Error::DatabaseCorrupted)),
                    Err(e) => Some(Err(e.into())),
                }
            }
            Ordering::Greater => Some(Err(Error::DatabaseCorrupted)),
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

struct ReadBytesCounter<R> {
    inner: R,
    num_bytes_read: usize,
}

impl<R: Read> ReadBytesCounter<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            num_bytes_read: 0,
        }
    }

    fn num_bytes_read(&self) -> usize {
        self.num_bytes_read
    }
}

impl<R: Read> Read for ReadBytesCounter<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let num_bytes_read = self.inner.read(buf)?;
        self.num_bytes_read += num_bytes_read;
        Ok(num_bytes_read)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        self.inner.read_exact(buf)?;
        self.num_bytes_read += buf.len();
        Ok(())
    }
}
