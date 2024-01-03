mod checkpoint_reader;
mod checkpoint_writer;
mod file_id;
mod io_monitor;
mod log_reader;
mod log_writer;
mod recovery;

pub use checkpoint_writer::Config as CheckpointerConfig;
pub use io_monitor::IoMonitor;
pub use log_writer::{Config as LoggerConfig, LogEntry, LogWriter, PersistentEpoch};
pub use recovery::recover;

use crate::{file_lock::FileLock, record::Record, Epoch, Result};
use checkpoint_writer::Checkpointer;
use file_id::FileId;
use log_writer::{LogWriterLock, Logger};
use std::{io::Write, path::Path, sync::Arc};

pub struct Persistence {
    persistent_epoch: Arc<PersistentEpoch>,
    logger: Logger,
    _checkpointer: Checkpointer,
    _lock: FileLock,
}

impl Persistence {
    pub fn new<R: Record>(
        lock: FileLock,
        persistent_epoch: Arc<PersistentEpoch>,
        logger_config: LoggerConfig,
        checkpointer_config: CheckpointerConfig<R>,
    ) -> Result<Self> {
        Ok(Self {
            persistent_epoch,
            logger: Logger::new(logger_config)?,
            _checkpointer: Checkpointer::new(checkpointer_config),
            _lock: lock,
        })
    }

    pub fn handle(&self) -> Result<PersistenceHandle<'_>> {
        Ok(PersistenceHandle {
            log_writer: self.logger.writer()?,
            persistent_epoch: &self.persistent_epoch,
        })
    }

    pub fn durable_epoch(&self) -> Epoch {
        self.persistent_epoch.get()
    }

    pub fn flush(&self) -> Result<Epoch> {
        self.logger.flush()
    }
}

pub struct PersistenceHandle<'a> {
    log_writer: LogWriter<'a>,
    persistent_epoch: &'a PersistentEpoch,
}

impl PersistenceHandle<'_> {
    pub fn lock_log_writer(&self) -> Result<LogWriterLock> {
        self.log_writer.lock()
    }

    pub fn wait_for_durability(&self, epoch: Epoch) {
        self.persistent_epoch.wait_for(epoch);
    }
}

struct WriteBytesCounter<W> {
    inner: W,
    num_bytes_written: usize,
}

impl<W> WriteBytesCounter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            num_bytes_written: 0,
        }
    }

    fn num_bytes_written(&self) -> usize {
        self.num_bytes_written
    }

    fn get_ref(&self) -> &W {
        &self.inner
    }

    fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: Write> Write for WriteBytesCounter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let num_bytes = self.inner.write(buf)?;
        self.num_bytes_written += num_bytes;
        Ok(num_bytes)
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.inner.write_all(buf)?;
        self.num_bytes_written += buf.len();
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

#[allow(clippy::unnecessary_wraps)]
fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    assert!(dir.is_dir());

    // FlushFileBuffers can't be used on directories.
    #[cfg(not(target_os = "windows"))]
    std::fs::File::open(dir)?.sync_data()?;

    Ok(())
}

/// Removes all files in the given directory that match the given predicate.
fn remove_files<F>(dir: &Path, predicate: F) -> std::io::Result<()>
where
    F: Fn(FileId) -> bool,
{
    assert!(dir.is_dir());
    for dir_entry in std::fs::read_dir(dir)? {
        let path = dir_entry?.path();
        let Some(id) = FileId::from_path(&path) else {
            continue;
        };
        if predicate(id) {
            std::fs::remove_file(path)?;
        }
    }
    Ok(())
}
