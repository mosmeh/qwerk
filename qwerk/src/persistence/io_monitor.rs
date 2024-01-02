use crate::{Error, Result};
use parking_lot::Mutex;
use std::sync::{
    atomic::{AtomicBool, Ordering::SeqCst},
    Arc,
};

/// A mechanism for monitoring I/O operation results.
///
/// This serves two purposes:
/// - Report errors encountered during background I/O operations, and
/// - Prevent further I/O operations after an error (foreground or background)
///   is encountered.
#[derive(Default)]
pub struct IoMonitor {
    encountered_error: AtomicBool,
    background_error: Mutex<Option<std::io::Error>>,
}

impl IoMonitor {
    /// Performs a foreground I/O operation in global scope.
    pub fn do_foreground<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce() -> std::io::Result<T>,
    {
        if self.encountered_error.load(SeqCst) {
            let e = self
                .background_error
                .lock()
                .take()
                .map_or(Error::PersistenceFailed, Into::into);
            return Err(e);
        }
        let result = f();
        if result.is_err() {
            self.encountered_error.store(true, SeqCst);
        }
        result.map_err(Into::into)
    }

    /// Performs a background I/O operation in global scope.
    ///
    /// Returns whether the operation was performed successfully.
    pub fn do_background<F>(&self, f: F) -> bool
    where
        F: FnOnce() -> std::io::Result<()>,
    {
        if self.encountered_error.load(SeqCst) {
            return false;
        }
        let result = f();
        match result {
            Ok(()) => true,
            Err(e) => {
                self.encountered_error.store(true, SeqCst);
                let mut error = self.background_error.lock();
                if error.is_none() {
                    *error = Some(e);
                }
                false
            }
        }
    }
}

/// A scope-local I/O operation monitor.
///
/// An error encountered in a scope-local I/O operation will not be reported
/// to the global monitor (`IoMonitor`), but will instead be reported only
/// to the scope-local monitor.
pub struct IoScope {
    monitor: Arc<IoMonitor>,
    background_error: Mutex<Option<std::io::Error>>,
}

impl IoScope {
    pub fn new(monitor: Arc<IoMonitor>) -> Self {
        Self {
            monitor,
            background_error: Default::default(),
        }
    }

    /// Performs a scope-local foreground I/O operation.
    pub fn do_foreground<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce() -> std::io::Result<T>,
    {
        if self.monitor.encountered_error.load(SeqCst) {
            let e = self
                .background_error
                .lock()
                .take()
                .or_else(|| self.monitor.background_error.lock().take())
                .map_or(Error::PersistenceFailed, Into::into);
            return Err(e);
        }
        let result = f();
        if result.is_err() {
            self.monitor.encountered_error.store(true, SeqCst);
        }
        result.map_err(Into::into)
    }

    /// Performs a scope-local background I/O operation.
    ///
    /// Returns whether the operation was performed successfully.
    pub fn do_background<F>(&self, f: F) -> bool
    where
        F: FnOnce() -> std::io::Result<()>,
    {
        if self.monitor.encountered_error.load(SeqCst) {
            return false;
        }
        let result = f();
        match result {
            Ok(()) => true,
            Err(e) => {
                self.monitor.encountered_error.store(true, SeqCst);
                let mut error = self.background_error.lock();
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
    use super::{IoMonitor, IoScope};
    use std::{
        io::{Error, ErrorKind},
        sync::Arc,
    };

    #[test]
    fn no_error() {
        let monitor = Arc::new(IoMonitor::default());
        let scope = IoScope::new(monitor.clone());
        assert!(monitor.do_background(|| Ok(())));
        assert!(monitor.do_foreground(|| Ok(())).is_ok());
        assert!(scope.do_background(|| Ok(())));
        assert!(scope.do_foreground(|| Ok(())).is_ok());
    }

    #[test]
    fn global_foreground_error() {
        let monitor = Arc::new(IoMonitor::default());
        let scope = IoScope::new(monitor.clone());
        assert!(monitor
            .do_foreground(|| -> std::io::Result<()> { Err(Error::new(ErrorKind::Other, "error")) })
            .is_err());
        assert!(monitor
            .do_foreground(|| -> std::io::Result<()> { unreachable!() })
            .is_err());
        assert!(scope.do_foreground(|| Ok(())).is_err());
        assert!(!scope.do_background(|| Ok(())));
    }

    #[test]
    fn global_background_error() {
        let monitor = Arc::new(IoMonitor::default());
        let scope = IoScope::new(monitor.clone());
        assert!(!monitor.do_background(|| Err(Error::new(ErrorKind::Other, "error"))));
        assert!(monitor.do_foreground(|| Ok(())).is_err());
        assert!(scope.do_foreground(|| Ok(())).is_err());
        assert!(!scope.do_background(|| Ok(())));
    }

    #[test]
    fn local_foreground_error() {
        let monitor = Arc::new(IoMonitor::default());
        let scope = IoScope::new(monitor.clone());
        assert!(scope
            .do_foreground(|| -> std::io::Result<()> { Err(Error::new(ErrorKind::Other, "error")) })
            .is_err());
        assert!(scope
            .do_foreground(|| -> std::io::Result<()> { unreachable!() })
            .is_err());
        assert!(monitor
            .do_foreground(|| -> std::io::Result<()> { unreachable!() })
            .is_err());
        assert!(!monitor.do_background(|| Ok(())));
    }

    #[test]
    fn local_background_error() {
        let monitor = Arc::new(IoMonitor::default());
        let scope = IoScope::new(monitor.clone());
        assert!(!scope.do_background(|| Err(Error::new(ErrorKind::Other, "error"))));
        assert!(scope
            .do_foreground(|| -> std::io::Result<()> { unreachable!() })
            .is_err());
        assert!(monitor
            .do_foreground(|| -> std::io::Result<()> { unreachable!() })
            .is_err());
        assert!(!monitor.do_background(|| Ok(())));
    }

    #[test]
    #[should_panic = "global-error"]
    fn scope_reports_global_error() {
        let monitor = Arc::new(IoMonitor::default());
        let scope = IoScope::new(monitor.clone());
        assert!(!monitor.do_background(|| Err(Error::new(ErrorKind::Other, "global-error"))));
        scope
            .do_foreground(|| -> std::io::Result<()> { unreachable!() })
            .unwrap();
    }

    #[test]
    #[should_panic = "scope-local-error"]
    fn scope_reports_local_error() {
        let monitor = Arc::new(IoMonitor::default());
        let scope = IoScope::new(monitor);
        assert!(!scope.do_background(|| Err(Error::new(ErrorKind::Other, "scope-local-error"))));
        scope
            .do_foreground(|| -> std::io::Result<()> { unreachable!() })
            .unwrap();
    }
}
