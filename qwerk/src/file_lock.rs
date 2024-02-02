use std::{fs::File, path::Path};

#[derive(Debug)]
pub struct FileLock {
    _file: File,
}

impl FileLock {
    /// Attempts to lock the file exclusively, creating it if it doesn't exist.
    ///
    /// # Returns
    ///
    /// `Ok(Some(_))` if the file was successfully locked.
    /// `Ok(None)` if the file is already locked.
    ///
    /// # Errors
    ///
    /// `Err` if the lock failed due to an IO error other than the file being
    /// already locked.
    pub fn try_lock_exclusive<P: AsRef<Path>>(path: P) -> std::io::Result<Option<Self>> {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.as_ref())?;
        Ok(sys::try_lock_exclusive(&file)?.then_some(Self { _file: file }))
    }
}

#[cfg(unix)]
mod sys {
    use std::{fs::File, os::fd::AsRawFd};

    pub fn try_lock_exclusive(file: &File) -> std::io::Result<bool> {
        let ret = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if ret < 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EWOULDBLOCK) {
                Ok(false)
            } else {
                Err(err)
            }
        } else {
            Ok(true)
        }
    }
}

#[cfg(windows)]
mod sys {
    use std::{fs::File, os::windows::io::AsRawHandle};
    use windows_sys::Win32::{
        Foundation::{ERROR_LOCK_VIOLATION, HANDLE},
        Storage::FileSystem::{LockFileEx, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY},
    };

    pub fn try_lock_exclusive(file: &File) -> std::io::Result<bool> {
        let mut overlapped = unsafe { std::mem::zeroed() };
        let ret = unsafe {
            LockFileEx(
                file.as_raw_handle() as HANDLE,
                LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
                0,
                !0,
                !0,
                &mut overlapped,
            )
        };
        if ret == 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(ERROR_LOCK_VIOLATION.try_into().unwrap()) {
                Ok(false)
            } else {
                Err(err)
            }
        } else {
            Ok(true)
        }
    }
}

#[cfg(not(any(unix, windows)))]
compile_error!("unsupported platform");

#[cfg(test)]
mod tests {
    use super::FileLock;
    use std::fs::File;
    use tempfile::tempdir;

    #[test]
    fn lock_new_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("lock");
        {
            let _lock = FileLock::try_lock_exclusive(&path).unwrap().unwrap();
            assert!(FileLock::try_lock_exclusive(&path).unwrap().is_none());
        }
        FileLock::try_lock_exclusive(path).unwrap().unwrap();
    }

    #[test]
    fn lock_existing_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("lock");
        let _file = File::create(&path).unwrap();
        {
            let _lock = FileLock::try_lock_exclusive(&path).unwrap().unwrap();
            assert!(FileLock::try_lock_exclusive(&path).unwrap().is_none());
        }
        FileLock::try_lock_exclusive(path).unwrap().unwrap();
    }

    #[test]
    fn lock_readonly_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("lock");
        let file = File::create(&path).unwrap();

        let mut permissions = file.metadata().unwrap().permissions();
        permissions.set_readonly(true);
        file.set_permissions(permissions).unwrap();

        let err = FileLock::try_lock_exclusive(&path).unwrap_err();
        assert!(err.kind() == std::io::ErrorKind::PermissionDenied);
    }
}
