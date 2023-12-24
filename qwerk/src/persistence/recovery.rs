use super::log_reader::LogReader;
use crate::{record::Record, ConcurrencyControl, Epoch, Index, Result};
use crossbeam_queue::SegQueue;
use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
};

pub fn recover<C: ConcurrencyControl>(
    dir: &Path,
    durable_epoch: Epoch,
    num_threads: NonZeroUsize,
) -> Result<Index<C::Record>> {
    let queue = SegQueue::new();
    for dir_entry in std::fs::read_dir(dir)? {
        let path = dir_entry?.path();
        if super::is_log_file(&path) {
            queue.push(path);
        }
    }
    if queue.is_empty() {
        return Ok(Default::default());
    }

    let num_threads = num_threads.get().min(queue.len());
    let shared = Arc::new(SharedState {
        index: Default::default(),
        queue,
    });

    let threads: Vec<_> = (0..num_threads)
        .map(|_| {
            let shared = shared.clone();
            std::thread::spawn(move || run_log_replayer::<C>(&shared, durable_epoch))
        })
        .collect();
    for thread in threads {
        thread.join().unwrap()?;
    }

    let SharedState { index, queue } = Arc::into_inner(shared).expect("all threads have exited");
    assert!(queue.is_empty());

    index.retain(|_, record_ptr| {
        let is_tombstone = unsafe { record_ptr.as_ref() }.is_tombstone();
        if is_tombstone {
            unsafe { record_ptr.drop_in_place() };
        }
        !is_tombstone
    });

    // TODO: replace log files with checkpoints. For now, just delete the log files.
    for dir_entry in std::fs::read_dir(dir)? {
        let path = dir_entry?.path();
        if super::is_log_file(&path) {
            std::fs::remove_file(path)?;
        }
    }

    Ok(index)
}

struct SharedState<T: 'static> {
    index: Index<T>,
    queue: SegQueue<PathBuf>,
}

fn run_log_replayer<C: ConcurrencyControl>(
    shared: &SharedState<C::Record>,
    durable_epoch: Epoch,
) -> Result<()> {
    while let Some(path) = shared.queue.pop() {
        let reader = LogReader::new(&path)?;
        for txn in reader {
            let txn = txn?;
            if txn.tid.epoch() > durable_epoch {
                continue;
            }
            for entry in txn.entries {
                C::load_log_entry(&shared.index, entry.key, entry.value, txn.tid);
            }
        }
    }
    Ok(())
}
