use crate::{
    bytes_ext::{ReadBytesExt, WriteBytesExt},
    memory_reclamation::{MemoryReclamation, Reclaimer},
    record::Record,
    signal_channel,
    small_bytes::SmallBytes,
    tid::Tid,
    ConcurrencyControl, Error, Index, Result,
};
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
    thread::JoinHandle,
    time::Duration,
};

pub struct Config<C: ConcurrencyControl> {
    pub dir: PathBuf,
    pub index: Arc<Index<C::Record>>,
    pub reclamation: Arc<MemoryReclamation>,
    pub interval: Duration,
}

pub struct Checkpointer {
    thread: Option<JoinHandle<()>>,
    stop_tx: signal_channel::Sender,
}

impl Checkpointer {
    pub fn new<C: ConcurrencyControl>(config: Config<C>) -> Self {
        let (stop_tx, stop_rx) = signal_channel::channel();
        let thread = {
            std::thread::Builder::new()
                .name("checkpointer".into())
                .spawn(move || {
                    let mut reclaimer = config.reclamation.reclaimer();
                    for _ in stop_rx.tick(config.interval) {
                        checkpoint::<C>(&config.dir, &config.index, &mut reclaimer).unwrap();
                    }
                })
                .unwrap()
        };
        Self {
            thread: Some(thread),
            stop_tx,
        }
    }
}

impl Drop for Checkpointer {
    fn drop(&mut self) {
        self.stop_tx.send();
        self.thread.take().unwrap().join().unwrap();
    }
}

const CHECKPOINT_FILE_NAME: &str = "checkpoint";

pub fn checkpoint<C: ConcurrencyControl>(
    dir: &Path,
    index: &Index<C::Record>,
    reclaimer: &mut Reclaimer<C::Record>,
) -> std::io::Result<()> {
    let tmp_path = dir.join("checkpoint.tmp");
    let mut file = BufWriter::new(File::create(&tmp_path)?);

    let mut num_entries = 0;
    {
        let enter_guard = reclaimer.enter();
        let mut maybe_entry = index.first_entry();
        while let Some(entry) = maybe_entry {
            let record_ptr = *entry.get();
            unsafe { record_ptr.as_ref() }
                .peek(|value, tid| -> std::io::Result<()> {
                    file.write_bytes(entry.key())?;
                    file.write_bytes(value)?;
                    file.write_u64(tid.0)?;
                    num_entries += 1;
                    Ok(())
                })
                .transpose()?;
            enter_guard.quiesce();
            maybe_entry = entry.next();
        }
    }

    file.write_u64(num_entries)?;
    file.into_inner()?.sync_data()?;

    // Atomically replace the file.
    std::fs::rename(&tmp_path, dir.join(CHECKPOINT_FILE_NAME))?;

    // Persist the rename operation.
    #[cfg(not(target_os = "windows"))] // FlushFileBuffers can't be used on directories.
    File::open(dir)?.sync_data()?;

    Ok(())
}

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
    pub fn new(dir: &Path) -> std::io::Result<Self> {
        let mut file = File::open(dir.join(CHECKPOINT_FILE_NAME))?;
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
