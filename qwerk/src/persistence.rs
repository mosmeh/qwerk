mod checkpoint;
mod log_reader;
mod log_writer;
mod recovery;

pub use checkpoint::Config as CheckpointerConfig;
pub use log_writer::{Config as LoggerConfig, LogEntry, LogWriter, PersistentEpoch};
pub use recovery::recover;

use crate::{file_lock::FileLock, record::Record, Epoch, Result};
use checkpoint::Checkpointer;
use log_writer::Logger;
use std::{io::Write, path::Path, str::FromStr, sync::Arc};

const MAX_FILE_SIZE: usize = 32 * 1024 * 1024;

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

    pub fn handle(&self) -> std::io::Result<PersistenceHandle<'_>> {
        Ok(PersistenceHandle {
            log_writer: self.logger.writer()?,
            persistent_epoch: &self.persistent_epoch,
        })
    }

    pub fn durable_epoch(&self) -> Epoch {
        self.persistent_epoch.get()
    }

    pub fn flush(&self) -> std::io::Result<Epoch> {
        self.logger.flush()
    }
}

pub struct PersistenceHandle<'a> {
    log_writer: LogWriter<'a>,
    persistent_epoch: &'a PersistentEpoch,
}

impl<'a> PersistenceHandle<'a> {
    pub fn log_writer(&self) -> &LogWriter<'a> {
        &self.log_writer
    }

    pub fn wait_for_durability(&self, epoch: Epoch) {
        self.persistent_epoch.wait_for(epoch);
    }
}

const CHECKPOINT_FILE_NAME_PREFIX: &str = "checkpoint_";
const LOG_FILE_NAME_PREFIX: &str = "log_";

enum FileId {
    Checkpoint(CheckpointFileId),
    Log(LogFileId),
    Temporary,
}

impl FileId {
    fn from_path(path: &Path) -> Option<Self> {
        fn parse<T: FromStr>(bytes: &[u8]) -> Option<T> {
            std::str::from_utf8(bytes).ok()?.parse().ok()
        }

        if !path.is_file() {
            return None;
        }
        let Some(name) = path.file_name() else {
            return None;
        };
        let name = name.as_encoded_bytes();
        if name.ends_with(b".tmp") {
            return Some(Self::Temporary);
        }
        if let Some(name) = name.strip_prefix(CHECKPOINT_FILE_NAME_PREFIX.as_bytes()) {
            let mut parts = name.splitn(2, |b| *b == b'_');
            let start_epoch = parse(parts.next()?)?;
            let id = match parts.next()? {
                b"last" => CheckpointFileId::Last { start_epoch },
                part => CheckpointFileId::Split {
                    start_epoch,
                    split_index: parse(part)?,
                },
            };
            return Some(Self::Checkpoint(id));
        }
        if let Some(name) = name.strip_prefix(LOG_FILE_NAME_PREFIX.as_bytes()) {
            let mut parts = name.splitn(2, |b| *b == b'_');
            let channel_index = parse(parts.next()?)?;
            let id = match parts.next()? {
                b"current" => LogFileId::Current { channel_index },
                name => LogFileId::Archive {
                    channel_index,
                    max_epoch: parse(name)?,
                },
            };
            return Some(Self::Log(id));
        }
        None
    }
}

enum CheckpointFileId {
    /// Split of a checkpoint.
    Split {
        start_epoch: Epoch,
        split_index: usize,
    },
    /// Last split of a checkpoint.
    ///
    /// The existence of this file indicates that the checkpoint is complete.
    Last { start_epoch: Epoch },
}

impl CheckpointFileId {
    fn start_epoch(&self) -> Epoch {
        match self {
            Self::Split { start_epoch, .. } | Self::Last { start_epoch } => *start_epoch,
        }
    }

    fn file_name(&self) -> String {
        match self {
            Self::Split {
                start_epoch,
                split_index,
            } => {
                format!("{CHECKPOINT_FILE_NAME_PREFIX}{start_epoch}_{split_index}")
            }
            Self::Last { start_epoch } => {
                format!("{CHECKPOINT_FILE_NAME_PREFIX}{start_epoch}_last")
            }
        }
    }
}

enum LogFileId {
    /// Log file that has been archived.
    Archive {
        channel_index: usize,
        max_epoch: Epoch,
    },
    /// Log file that is currently being written to.
    Current { channel_index: usize },
}

impl LogFileId {
    fn file_name(&self) -> String {
        match self {
            Self::Archive {
                channel_index,
                max_epoch,
            } => format!("{LOG_FILE_NAME_PREFIX}{channel_index}_{max_epoch}"),
            Self::Current { channel_index } => {
                format!("{LOG_FILE_NAME_PREFIX}{channel_index}_current")
            }
        }
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
