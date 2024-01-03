use super::{
    checkpoint_reader::CheckpointReader,
    checkpoint_writer::non_fuzzy_checkpoint,
    file_id::{CheckpointFileId, FileId, LogFileId},
    log_reader::LogReader,
    PersistentEpoch,
};
use crate::{record::Record, ConcurrencyControl, Epoch, Index, Result};
use crossbeam_queue::ArrayQueue;
use std::{cmp::Ordering, num::NonZeroUsize, path::Path, sync::Arc};

/// Recovers the database from the given directory.
///
/// Returns the index and the initial epoch that should be used for the
/// recovered database.
pub fn recover<C: ConcurrencyControl>(
    dir: &Path,
    persistent_epoch: &PersistentEpoch,
    num_threads: NonZeroUsize,
    max_file_size: usize,
) -> Result<(Index<C::Record>, Epoch)> {
    let durable_epoch = persistent_epoch.get();
    let mut latest_checkpoint_epoch = None;
    let mut checkpoint_files = Vec::new();
    let mut log_files = Vec::new();

    for dir_entry in std::fs::read_dir(dir)? {
        let path = dir_entry?.path();
        let Some(file_id) = FileId::from_path(&path) else {
            continue;
        };
        match file_id {
            FileId::Checkpoint(id) => {
                if let CheckpointFileId::Last { start_epoch } = id {
                    if latest_checkpoint_epoch.map_or(true, |latest| start_epoch > latest) {
                        latest_checkpoint_epoch = Some(start_epoch);
                    }
                }
                checkpoint_files.push(id);
            }
            FileId::Log(id) => log_files.push(id),
            FileId::Temporary => (),
        }
    }

    let index = Arc::new(Index::new());
    if let Some(checkpoint_epoch) = latest_checkpoint_epoch {
        assert!(!checkpoint_files.is_empty());
        let files_to_load = ArrayQueue::new(checkpoint_files.len());
        for id in checkpoint_files {
            if id.start_epoch() == checkpoint_epoch {
                files_to_load.push(id).unwrap();
            }
        }
        assert!(!files_to_load.is_empty());
        let index = index.clone();
        load_files(files_to_load, num_threads, move |id| {
            let reader = CheckpointReader::new(dir, &id)?;
            for entry in reader {
                let entry = entry?;
                C::load_record(&index, entry.key, Some(entry.value), entry.tid);
            }
            Ok(())
        })?;
    }

    if let Some(checkpoint_epoch) = latest_checkpoint_epoch {
        log_files.retain(|id| match id {
            LogFileId::Archive { max_epoch, .. } => *max_epoch >= checkpoint_epoch,
            LogFileId::Current { .. } => true,
        });
    }
    if !log_files.is_empty() {
        // Sort log files by max epoch in descending order to load the most
        // recent log files first. By doing so, we can avoid loading log entries
        // that are overwritten by later log entries.
        log_files.sort_unstable_by(|a, b| match (a, b) {
            (LogFileId::Archive { max_epoch: a, .. }, LogFileId::Archive { max_epoch: b, .. }) => {
                b.cmp(a)
            }
            (LogFileId::Archive { .. }, LogFileId::Current { .. }) => Ordering::Greater,
            (LogFileId::Current { .. }, LogFileId::Archive { .. }) => Ordering::Less,
            (LogFileId::Current { .. }, LogFileId::Current { .. }) => Ordering::Equal,
        });
        let files_to_load = ArrayQueue::new(log_files.len());
        for id in log_files {
            files_to_load.push(id).unwrap();
        }
        let index = index.clone();
        load_files(files_to_load, num_threads, move |id| {
            let reader = LogReader::new(dir, &id)?;
            for entry in reader {
                let entry = entry?;
                let epoch = entry.tid.epoch();
                if epoch > durable_epoch {
                    continue;
                }
                match latest_checkpoint_epoch {
                    Some(checkpoint_epoch) if epoch < checkpoint_epoch => continue,
                    _ => (),
                }
                for record in entry.records {
                    C::load_record(&index, record.key, record.value, entry.tid);
                }
            }
            Ok(())
        })?;
    }

    let index = Arc::into_inner(index).expect("all threads have exited");

    // Unlike normal transaction operations, load_record does not remove
    // tombstones. We remove them here.
    index.retain(|_, record_ptr| {
        let is_tombstone = unsafe { record_ptr.as_ref() }.is_tombstone();
        if is_tombstone {
            unsafe { record_ptr.drop_in_place() };
        }
        !is_tombstone
    });

    // Decide a new checkpoint epoch that is distinct from the existing ones.
    let mut new_checkpoint_epoch = durable_epoch.increment();
    if let Some(checkpoint_epoch) = latest_checkpoint_epoch {
        new_checkpoint_epoch = new_checkpoint_epoch.max(checkpoint_epoch.increment());
    }
    let index = non_fuzzy_checkpoint(dir, index, new_checkpoint_epoch, max_file_size)?;

    // Remove old checkpoint files, log files, and unfinished temporary files.
    for dir_entry in std::fs::read_dir(dir)? {
        let path = dir_entry?.path();
        let Some(file_id) = FileId::from_path(&path) else {
            continue;
        };
        let should_remove = match file_id {
            FileId::Checkpoint(id) => id.start_epoch() < new_checkpoint_epoch,
            FileId::Log(_) | FileId::Temporary => true,
        };
        if should_remove {
            std::fs::remove_file(path)?;
        }
    }

    Ok((index, new_checkpoint_epoch.increment()))
}

fn load_files<T, F>(files: ArrayQueue<T>, num_threads: NonZeroUsize, load: F) -> Result<()>
where
    T: Send,
    F: Fn(T) -> Result<()> + Send + Clone,
{
    let num_threads = num_threads.get().min(files.len());
    let queue = Arc::new(files);
    std::thread::scope(|s| -> Result<()> {
        let threads: Vec<_> = (0..num_threads)
            .map(|_| {
                let queue = queue.clone();
                let load = load.clone();
                s.spawn(move || -> Result<()> {
                    while let Some(file) = queue.pop() {
                        load(file)?;
                    }
                    Ok(())
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap()?;
        }
        Ok(())
    })?;
    assert!(queue.is_empty());
    Ok(())
}
