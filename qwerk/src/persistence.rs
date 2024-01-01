mod checkpoint;
mod log_reader;
mod log_writer;
mod recovery;

pub use checkpoint::Config as CheckpointerConfig;
pub use log_writer::{Config as LoggerConfig, LogEntry, LogWriter, PersistentEpoch};
pub use recovery::recover;

use crate::{file_lock::FileLock, ConcurrencyControl, Epoch, Result};
use checkpoint::Checkpointer;
use log_writer::Logger;
use std::{path::Path, sync::Arc};

const LOG_FILE_NAME_PREFIX: &str = "log_";

fn is_log_file(path: &Path) -> bool {
    if !path.is_file() {
        return false;
    }

    let Some(name) = path.file_name() else {
        return false;
    };
    let name = name
        .as_encoded_bytes()
        .strip_prefix(LOG_FILE_NAME_PREFIX.as_bytes());
    let Some(name) = name else {
        return false;
    };
    name.iter().all(u8::is_ascii_digit)
}

pub struct Persistence {
    persistent_epoch: Arc<PersistentEpoch>,
    logger: Logger,
    _checkpointer: Checkpointer,
    _lock: FileLock,
}

impl Persistence {
    pub fn new<C: ConcurrencyControl>(
        lock: FileLock,
        persistent_epoch: Arc<PersistentEpoch>,
        logger_config: LoggerConfig,
        checkpointer_config: CheckpointerConfig<C>,
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
