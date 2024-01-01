//! Checkpointing.
//!
//! There are two types of checkpoints: non-fuzzy (consistent) and fuzzy
//! (inconsistent).
//! Non-fuzzy checkpoints are point-in-time snapshots of the database.
//! Fuzzy checkpoints are created while there are concurrent writes to the
//! database. They do not represent a point-in-time snapshot of the database
//! by themselves, and should be combined with the log files to recover
//! the consistent state of the database.

use super::{
    CheckpointFileId, FileId, LogFileId, PersistentEpoch, WriteBytesCounter, MAX_FILE_SIZE,
};
use crate::{
    bytes_ext::{ReadBytesExt, WriteBytesExt},
    epoch::EpochFramework,
    memory_reclamation::{MemoryReclamation, Reclaimer},
    record::Record,
    signal_channel,
    small_bytes::SmallBytes,
    tid::Tid,
    Epoch, Error, Index, Result,
};
use scc::hash_index::OccupiedEntry;
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
    thread::JoinHandle,
    time::Duration,
};

pub struct Config<R: Record> {
    pub dir: PathBuf,
    pub index: Arc<Index<R>>,
    pub epoch_fw: Arc<EpochFramework>,
    pub reclamation: Arc<MemoryReclamation>,
    pub persistent_epoch: Arc<PersistentEpoch>,
    pub interval: Duration,
}

pub struct Checkpointer {
    thread: Option<JoinHandle<()>>,
    stop_tx: signal_channel::Sender,
}

impl Checkpointer {
    pub fn new<R: Record>(config: Config<R>) -> Self {
        let (stop_tx, stop_rx) = signal_channel::channel();
        let thread = {
            std::thread::Builder::new()
                .name("checkpointer".into())
                .spawn(move || {
                    let mut reclaimer = config.reclamation.reclaimer();
                    let mut last_epoch = None;
                    for _ in stop_rx.tick(config.interval) {
                        let mut epoch = config.epoch_fw.global_epoch();
                        while last_epoch.map_or(false, |last| epoch <= last) {
                            epoch = config.epoch_fw.sync();
                        }
                        fuzzy_checkpoint(
                            &config.dir,
                            &config.index,
                            &config.persistent_epoch,
                            &mut reclaimer,
                            epoch,
                        )
                        .unwrap(); // TODO: Handle errors.
                        last_epoch = Some(epoch);
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

/// Creates a non-fuzzy checkpoint.
///
/// This function takes an ownership of `index` and returns it back to make sure
/// that there are no concurrent accesses to `index`.
pub fn non_fuzzy_checkpoint<R: Record>(
    dir: &Path,
    index: Index<R>,
    epoch: Epoch,
) -> std::io::Result<Index<R>> {
    // We don't have any concurrent accesses to the index, so we can use a dummy
    // reclaimer.
    let reclamation = MemoryReclamation::new(usize::MAX);
    let mut reclaimer = reclamation.reclaimer();

    checkpoint(dir, &index, None, &mut reclaimer, epoch)?;
    Ok(index)
}

/// Creates a fuzzy checkpoint.
///
/// Concurrent accesses to `index` should use `MemoryReclamation` that
/// `reclaimer` is associated with.
fn fuzzy_checkpoint<R: Record>(
    dir: &Path,
    index: &Index<R>,
    persistent_epoch: &PersistentEpoch,
    reclaimer: &mut Reclaimer<R>,
    start_epoch: Epoch,
) -> std::io::Result<()> {
    checkpoint(dir, index, Some(persistent_epoch), reclaimer, start_epoch)
}

fn checkpoint<R: Record>(
    dir: &Path,
    index: &Index<R>,
    persistent_epoch: Option<&PersistentEpoch>,
    reclaimer: &mut Reclaimer<R>,
    start_epoch: Epoch,
) -> std::io::Result<()> {
    let mut cursor: Option<OccupiedEntry<_, _>> = None;
    let mut max_epoch = None;
    let mut split_index = 0;

    loop {
        let path = dir.join(
            CheckpointFileId::Split {
                start_epoch,
                split_index,
            }
            .file_name(),
        );
        let mut file = WriteBytesCounter::new(BufWriter::new(File::create(path)?));

        let mut num_entries = 0;
        {
            let enter_guard = reclaimer.enter();
            cursor = cursor.map_or_else(|| index.first_entry(), OccupiedEntry::next);
            while let Some(entry) = cursor {
                let record_ptr = *entry.get();
                let epoch = unsafe { record_ptr.as_ref() }
                    .peek(|value, tid| -> std::io::Result<_> {
                        if tid.epoch() >= start_epoch {
                            return Ok(None);
                        }
                        file.write_bytes(entry.key())?;
                        file.write_bytes(value)?;
                        file.write_u64(tid.0)?;
                        num_entries += 1;
                        Ok(Some(tid.epoch()))
                    })
                    .transpose()?
                    .flatten();
                enter_guard.quiesce();
                max_epoch = max_epoch.max(epoch);
                if file.num_bytes_written() >= MAX_FILE_SIZE {
                    cursor = Some(entry);
                    break;
                }
                let Some(next_cursor) = entry.next() else {
                    cursor = None;
                    break;
                };
                cursor = Some(next_cursor);
            }
        }

        let mut file = file.into_inner();
        file.write_u64(num_entries)?;
        file.into_inner()?.sync_data()?;

        if cursor.is_none() {
            // We've reached the end of the index.
            break;
        }
        split_index += 1;
    }

    if let (Some(persistent_epoch), Some(max_epoch)) = (persistent_epoch, max_epoch) {
        persistent_epoch.wait_for(max_epoch);
    }

    std::fs::rename(
        dir.join(
            CheckpointFileId::Split {
                start_epoch,
                split_index,
            }
            .file_name(),
        ),
        dir.join(CheckpointFileId::Last { start_epoch }.file_name()),
    )?;

    // Persist the rename.
    #[cfg(not(target_os = "windows"))] // FlushFileBuffers can't be used on directories.
    File::open(dir)?.sync_data()?;

    // Remove old checkpoint and log files.
    for dir_entry in std::fs::read_dir(dir)? {
        let path = dir_entry?.path();
        let Some(file_id) = FileId::from_path(&path) else {
            continue;
        };
        let should_remove = match file_id {
            FileId::Checkpoint(id) if id.start_epoch() < start_epoch => true,
            FileId::Log(LogFileId::Archive { max_epoch, .. }) if max_epoch < start_epoch => true,
            _ => false,
        };
        if should_remove {
            std::fs::remove_file(path)?;
        }
    }

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
