//! Checkpoint writer.
//!
//! There are two types of checkpoints: non-fuzzy (consistent) and fuzzy
//! (inconsistent).
//! Non-fuzzy checkpoints are point-in-time snapshots of the database.
//! Fuzzy checkpoints are created while there are concurrent writes to the
//! database. They do not represent a point-in-time snapshot of the database
//! by themselves, and should be combined with the log files to recover
//! the consistent state of the database.

use super::{
    file_id::{CheckpointFileId, FileId, LogFileId},
    fsync_dir,
    io_monitor::IoMonitor,
    remove_files, PersistentEpoch, WriteBytesCounter,
};
use crate::{
    bytes_ext::WriteBytesExt,
    epoch::EpochFramework,
    memory_reclamation::{MemoryReclamation, Reclaimer},
    record::Record,
    signal_channel, Epoch, Index,
};
use scc::hash_index::OccupiedEntry;
use std::{
    fs::File,
    io::BufWriter,
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
    pub io_monitor: Arc<IoMonitor>,
    pub interval: Duration,
    pub max_file_size: usize,
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
                .spawn(move || run_checkpointer(config, stop_rx))
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

fn run_checkpointer<R: Record>(config: Config<R>, stop_rx: signal_channel::Receiver) {
    let mut reclaimer = config.reclamation.reclaimer();
    let mut last_epoch = None;
    for _ in stop_rx.tick(config.interval) {
        let mut epoch = config.epoch_fw.global_epoch();
        while last_epoch.map_or(false, |last| epoch <= last) {
            epoch = config.epoch_fw.sync();
        }
        if !config.io_monitor.do_background(|| {
            fuzzy_checkpoint(
                &config.dir,
                &config.index,
                &config.persistent_epoch,
                &mut reclaimer,
                epoch,
                config.max_file_size,
            )
        }) {
            return;
        }
        last_epoch = Some(epoch);
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
    max_file_size: usize,
) -> std::io::Result<Index<R>> {
    // We don't have any concurrent accesses to the index, so we can use a dummy
    // reclaimer.
    let reclamation = MemoryReclamation::new(usize::MAX);
    let mut reclaimer = reclamation.reclaimer();

    checkpoint(dir, &index, None, &mut reclaimer, epoch, max_file_size)?;
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
    max_file_size: usize,
) -> std::io::Result<()> {
    checkpoint(
        dir,
        index,
        Some(persistent_epoch),
        reclaimer,
        start_epoch,
        max_file_size,
    )
}

fn checkpoint<R: Record>(
    dir: &Path,
    index: &Index<R>,
    persistent_epoch: Option<&PersistentEpoch>,
    reclaimer: &mut Reclaimer<R>,
    start_epoch: Epoch,
    max_file_size: usize,
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
                if file.num_bytes_written() >= max_file_size {
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
    fsync_dir(dir)?;

    // Remove old checkpoint and log files.
    remove_files(dir, |id| match id {
        FileId::Checkpoint(id) if id.start_epoch() < start_epoch => true,
        FileId::Log(LogFileId::Archive { max_epoch, .. }) if max_epoch < start_epoch => true,
        _ => false,
    })?;

    Ok(())
}
