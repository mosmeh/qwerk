use crate::{bytes_ext::ReadBytesExt, small_bytes::SmallBytes, tid::Tid, Error, Result};
use std::{
    fs::File,
    io::{BufReader, Read, Seek, SeekFrom},
    path::Path,
};
pub struct CheckpointEntry {
    pub key: SmallBytes,
    pub value: Box<[u8]>,
    pub tid: Tid,
}

pub struct CheckpointReader {
    file: BufReader<File>,
    num_entries: u64,
    num_remaining_entries: u64,
}

impl CheckpointReader {
    pub fn new(path: &Path) -> std::io::Result<Self> {
        let mut file = File::open(path)?;
        file.seek(SeekFrom::End(
            -i64::try_from(std::mem::size_of::<u64>()).unwrap(),
        ))?;
        let num_entries = file.read_u64()?;
        file.seek(SeekFrom::Start(0))?;
        Ok(Self {
            file: BufReader::new(file),
            num_entries,
            num_remaining_entries: num_entries,
        })
    }

    fn read_entry(&mut self) -> Result<Option<CheckpointEntry>> {
        if self.num_remaining_entries == 0 {
            let num_entries_in_footer = self.file.read_u64()?;
            if num_entries_in_footer != self.num_entries {
                return Err(Error::DatabaseCorrupted);
            }

            // We should have reached EOF.
            return match self.file.read(&mut [0; 1]) {
                Ok(0) => Ok(None),
                Ok(_) => Err(Error::DatabaseCorrupted),
                Err(e) => Err(e.into()),
            };
        }

        let key = self.file.read_bytes()?;
        let value = self.file.read_bytes()?.into();
        let tid = Tid(self.file.read_u64()?);
        let entry = CheckpointEntry {
            key: key.into(),
            value,
            tid,
        };

        if let Some(n) = self.num_remaining_entries.checked_sub(1) {
            self.num_remaining_entries = n;
            Ok(Some(entry))
        } else {
            Err(Error::DatabaseCorrupted)
        }
    }
}

impl Iterator for CheckpointReader {
    type Item = Result<CheckpointEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read_entry().transpose()
    }
}
