use crate::{Error, Result};
use parking_lot::Mutex;
use std::sync::{
    atomic::{AtomicBool, Ordering::SeqCst},
    Arc,
};

/// I/O operation monitor.
///
/// This serves two purposes:
/// - Report errors encountered during background I/O operations, and
/// - Prevent further foreground I/O operations after
///   an error (foreground or background) is encountered.
///
/// Any encountered error (both in the parent and sub-monitors) prevents
/// further foreground I/O operations in all monitors.
///
/// Background I/O operations are not prevented so that background threads can
/// continue their work for graceful shutdown.
#[derive(Default)]
pub struct IoMonitor {
    global_background_error: Mutex<Option<std::io::Error>>,
    encountered_error: AtomicBool,
}

impl IoMonitor {
    /// Performs a foreground I/O operation.
    ///
    /// If an error has been encountered during a foreground or background I/O
    /// operation, this returns an error without performing the operation.
    pub fn do_foreground<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce() -> std::io::Result<T>,
    {
        if self.encountered_error.load(SeqCst) {
            let e = self
                .global_background_error
                .lock()
                .take()
                .map_or(Error::PersistenceFailed, Into::into);
            return Err(e);
        }
        let result = f();
        if result.is_err() {
            self.encountered_error.store(true, SeqCst);
        }
        Ok(result?)
    }

    /// Performs a background I/O operation.
    ///
    /// Returns whether the operation succeeded.
    pub fn do_background<F>(&self, f: F) -> bool
    where
        F: FnOnce() -> std::io::Result<()>,
    {
        let result = f();
        match result {
            Ok(()) => true,
            Err(e) => {
                self.encountered_error.store(true, SeqCst);
                let mut error = self.global_background_error.lock();
                if error.is_none() {
                    *error = Some(e);
                }
                false
            }
        }
    }
}

/// I/O operation monitor for a scope (e.g. a file).
///
/// Encountered errors are reported to the parent monitor, but the error object
/// is only returned by `IoSubMonitor::do_foreground` so that the caller can
/// determine the source of the error.
pub struct IoSubMonitor {
    parent: Arc<IoMonitor>,
    local_background_error: Mutex<Option<std::io::Error>>,
}

impl IoSubMonitor {
    pub fn new(parent: Arc<IoMonitor>) -> Self {
        Self {
            parent,
            local_background_error: None.into(),
        }
    }

    /// Performs a foreground I/O operation.
    ///
    /// If an error has been encountered during a foreground or background I/O
    /// operation, this returns an error without performing the operation.
    pub fn do_foreground<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce() -> std::io::Result<T>,
    {
        if self.parent.encountered_error.load(SeqCst) {
            let e = self
                .local_background_error
                .lock()
                .take()
                .or_else(|| self.parent.global_background_error.lock().take())
                .map_or(Error::PersistenceFailed, Into::into);
            return Err(e);
        }
        let result = f();
        if result.is_err() {
            self.parent.encountered_error.store(true, SeqCst);
        }
        Ok(result?)
    }

    /// Performs a background I/O operation.
    ///
    /// Returns whether the operation succeeded.
    pub fn do_background<F>(&self, f: F) -> bool
    where
        F: FnOnce() -> std::io::Result<()>,
    {
        let result = f();
        match result {
            Ok(()) => true,
            Err(e) => {
                self.parent.encountered_error.store(true, SeqCst);
                let mut error = self.local_background_error.lock();
                if error.is_none() {
                    *error = Some(e);
                }
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{IoMonitor, IoSubMonitor};
    use std::{
        io::{Error, ErrorKind},
        sync::Arc,
    };

    #[test]
    fn no_error() {
        let monitor = Arc::new(IoMonitor::default());
        let sub_monitor = IoSubMonitor::new(monitor.clone());
        assert!(monitor.do_background(|| Ok(())));
        assert!(monitor.do_foreground(|| Ok(())).is_ok());
        assert!(sub_monitor.do_background(|| Ok(())));
        assert!(sub_monitor.do_foreground(|| Ok(())).is_ok());
    }

    #[test]
    fn foreground_error_on_parent_monitor() {
        let monitor = Arc::new(IoMonitor::default());
        let sub_monitor = IoSubMonitor::new(monitor.clone());
        assert!(monitor
            .do_foreground(|| -> std::io::Result<()> { Err(Error::new(ErrorKind::Other, "error")) })
            .is_err());
        assert!(monitor
            .do_foreground(|| -> std::io::Result<()> { unreachable!() })
            .is_err());
        assert!(sub_monitor.do_foreground(|| Ok(())).is_err());
        assert!(sub_monitor.do_background(|| Ok(())));
    }

    #[test]
    fn background_error_on_parent_monitor() {
        let monitor = Arc::new(IoMonitor::default());
        let sub_monitor = IoSubMonitor::new(monitor.clone());
        assert!(!monitor.do_background(|| Err(Error::new(ErrorKind::Other, "error"))));
        assert!(monitor.do_foreground(|| Ok(())).is_err());
        assert!(sub_monitor.do_foreground(|| Ok(())).is_err());
        assert!(sub_monitor.do_background(|| Ok(())));
    }

    #[test]
    fn foreground_error_on_sub_monitor() {
        let monitor = Arc::new(IoMonitor::default());
        let sub_monitor = IoSubMonitor::new(monitor.clone());
        assert!(sub_monitor
            .do_foreground(|| -> std::io::Result<()> { Err(Error::new(ErrorKind::Other, "error")) })
            .is_err());
        assert!(sub_monitor
            .do_foreground(|| -> std::io::Result<()> { unreachable!() })
            .is_err());
        assert!(monitor
            .do_foreground(|| -> std::io::Result<()> { unreachable!() })
            .is_err());
        assert!(monitor.do_background(|| Ok(())));
    }

    #[test]
    fn background_error_on_sub_monitor() {
        let monitor = Arc::new(IoMonitor::default());
        let sub_monitor = IoSubMonitor::new(monitor.clone());
        assert!(!sub_monitor.do_background(|| Err(Error::new(ErrorKind::Other, "error"))));
        assert!(sub_monitor
            .do_foreground(|| -> std::io::Result<()> { unreachable!() })
            .is_err());
        assert!(monitor
            .do_foreground(|| -> std::io::Result<()> { unreachable!() })
            .is_err());
        assert!(monitor.do_background(|| Ok(())));
    }

    #[test]
    #[should_panic = "parent"]
    fn report_global_error() {
        let monitor = Arc::new(IoMonitor::default());
        let sub_monitor = IoSubMonitor::new(monitor.clone());
        assert!(!sub_monitor.do_background(|| Err(Error::new(ErrorKind::Other, "sub"))));
        assert!(!monitor.do_background(|| Err(Error::new(ErrorKind::Other, "parent"))));
        monitor
            .do_foreground(|| -> std::io::Result<()> { unreachable!() })
            .unwrap();
    }

    #[test]
    #[should_panic = "sub"]
    fn report_local_error() {
        let monitor = Arc::new(IoMonitor::default());
        let sub_monitor = IoSubMonitor::new(monitor.clone());
        assert!(!monitor.do_background(|| Err(Error::new(ErrorKind::Other, "parent"))));
        assert!(!sub_monitor.do_background(|| Err(Error::new(ErrorKind::Other, "sub"))));
        sub_monitor
            .do_foreground(|| -> std::io::Result<()> { unreachable!() })
            .unwrap();
    }
}
