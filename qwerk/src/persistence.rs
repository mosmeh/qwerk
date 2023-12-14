mod log_reader;
mod log_writer;
mod recovery;

pub use log_writer::{Config as LoggerConfig, DurableEpoch, LogWriter, Logger};
pub use recovery::recover;

use std::path::Path;

const LOG_FILE_NAME_PREFIX: &str = "log_";

pub fn is_log_file(path: &Path) -> bool {
    if !path.is_file() {
        return false;
    }

    const PREFIX: &[u8] = LOG_FILE_NAME_PREFIX.as_bytes();
    path.file_name()
        .map(|name| name.as_encoded_bytes().starts_with(PREFIX))
        .unwrap_or_default()
}
