use std::{fs::File, path::Path};

pub struct FileLock {
    _file: File,
}

impl FileLock {
    /// Attempts to lock the file exclusively.
    ///
    /// Returns an RAII guard if the lock succeeded.
    ///
    /// # Errors
    /// If the file is already locked, `Ok(None)` will be returned.
    /// If the lock failed for other reasons, `Err` will be returned.
    pub fn try_lock_exclusive<P: AsRef<Path>>(path: P) -> std::io::Result<Option<Self>> {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
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
