use crate::Epoch;
use std::{ffi::OsStr, path::Path, str::FromStr};

#[derive(Debug, PartialEq, Eq)]
pub enum FileId {
    Checkpoint(CheckpointFileId),
    Log(LogFileId),
    Temporary,
}

impl FileId {
    pub fn from_path(path: &Path) -> Option<Self> {
        fn parse<T: FromStr>(bytes: &[u8]) -> Option<T> {
            std::str::from_utf8(bytes).ok()?.parse().ok()
        }

        if !path.is_file() {
            return None;
        }
        if path.extension().map(OsStr::as_encoded_bytes) == Some(b"tmp") {
            return Some(Self::Temporary);
        }
        let name = path.file_name()?.as_encoded_bytes();
        if let Some(name) = name.strip_prefix(CheckpointFileId::FILE_NAME_PREFIX.as_bytes()) {
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
        if let Some(name) = name.strip_prefix(LogFileId::FILE_NAME_PREFIX.as_bytes()) {
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

#[derive(Debug, PartialEq, Eq)]
pub enum CheckpointFileId {
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
    const FILE_NAME_PREFIX: &'static str = "checkpoint_";

    pub fn start_epoch(&self) -> Epoch {
        match self {
            Self::Split { start_epoch, .. } | Self::Last { start_epoch } => *start_epoch,
        }
    }

    pub fn file_name(&self) -> String {
        match self {
            Self::Split {
                start_epoch,
                split_index,
            } => {
                format!(
                    "{prefix}{start_epoch}_{split_index}",
                    prefix = Self::FILE_NAME_PREFIX
                )
            }
            Self::Last { start_epoch } => {
                format!(
                    "{prefix}{start_epoch}_last",
                    prefix = Self::FILE_NAME_PREFIX
                )
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum LogFileId {
    /// Log file that has been archived.
    Archive {
        channel_index: usize,
        max_epoch: Epoch,
    },
    /// Log file that is currently being written to.
    Current { channel_index: usize },
}

impl LogFileId {
    const FILE_NAME_PREFIX: &'static str = "log_";

    pub fn file_name(&self) -> String {
        match self {
            Self::Archive {
                channel_index,
                max_epoch,
            } => format!(
                "{prefix}{channel_index}_{max_epoch}",
                prefix = Self::FILE_NAME_PREFIX
            ),
            Self::Current { channel_index } => {
                format!(
                    "{prefix}{channel_index}_current",
                    prefix = Self::FILE_NAME_PREFIX
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CheckpointFileId, FileId, LogFileId};
    use crate::Epoch;
    use std::fs::{DirBuilder, File};

    #[test]
    fn checkpoint() {
        let id = CheckpointFileId::Split {
            start_epoch: Epoch(123),
            split_index: 456,
        };
        assert_eq!(id.file_name(), "checkpoint_123_456");
        assert_eq!(id.start_epoch(), Epoch(123));

        let id = CheckpointFileId::Last {
            start_epoch: Epoch(123),
        };
        assert_eq!(id.file_name(), "checkpoint_123_last");
        assert_eq!(id.start_epoch(), Epoch(123));
    }

    #[test]
    fn log() {
        let id = LogFileId::Archive {
            channel_index: 123,
            max_epoch: Epoch(456),
        };
        assert_eq!(id.file_name(), "log_123_456");

        let id = LogFileId::Current { channel_index: 123 };
        assert_eq!(id.file_name(), "log_123_current");
    }

    #[test]
    fn from_path() {
        fn parse(file_name: &str) -> FileId {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join(file_name);
            File::create(&path).unwrap();
            FileId::from_path(&path).unwrap()
        }

        assert_eq!(
            parse("checkpoint_123_456"),
            FileId::Checkpoint(CheckpointFileId::Split {
                start_epoch: Epoch(123),
                split_index: 456,
            })
        );
        assert_eq!(
            parse("checkpoint_123_last"),
            FileId::Checkpoint(CheckpointFileId::Last {
                start_epoch: Epoch(123),
            })
        );
        assert_eq!(
            parse("log_123_456"),
            FileId::Log(LogFileId::Archive {
                channel_index: 123,
                max_epoch: Epoch(456),
            })
        );
        assert_eq!(
            parse("log_123_current"),
            FileId::Log(LogFileId::Current { channel_index: 123 })
        );
        assert_eq!(parse("foo.tmp"), FileId::Temporary);

        // Non-file path.
        let dir = tempfile::tempdir().unwrap();
        DirBuilder::new()
            .create(dir.path().join("checkpoint_123_456"))
            .unwrap();
        assert!(FileId::from_path(dir.path()).is_none());
    }
}
