mod log_reader;
mod log_writer;
mod recovery;

pub use log_writer::{Config as LoggerConfig, LogEntry, LogWriter, Logger, PersistentEpoch};
pub use recovery::recover;

use std::path::Path;

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
