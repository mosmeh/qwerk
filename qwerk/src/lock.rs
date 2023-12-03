use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

// bit [0]:     whether exclusive lock is held
// bits[1..63]: number of shared locks held

const WRITER: u64 = 0x1;
const READER: u64 = 0x2;
const READER_FULL: u64 = !WRITER;

/// A reader-writer lock.
pub struct Lock(AtomicU64);

impl Lock {
    pub const fn new_locked_exclusive() -> Self {
        Self(AtomicU64::new(WRITER))
    }

    pub fn is_locked(&self) -> bool {
        self.0.load(SeqCst) != 0
    }

    pub fn is_locked_exclusive(&self) -> bool {
        self.0.load(SeqCst) & WRITER != 0
    }

    pub fn try_lock_shared(&self) -> bool {
        let current = self.0.load(SeqCst);
        if current & WRITER != 0 || current == READER_FULL {
            return false;
        }
        self.0
            .compare_exchange_weak(current, current + READER, SeqCst, SeqCst)
            .is_ok()
    }

    pub fn try_lock_exclusive(&self) -> bool {
        if self.0.load(SeqCst) != 0 {
            return false;
        }
        self.0
            .compare_exchange_weak(0, WRITER, SeqCst, SeqCst)
            .is_ok()
    }

    // parking_lot doesn't allow upgrading read lock to write lock, and only
    // "upgradable read" locks can be upgraded. This is to avoid two read locks
    // from trying to upgrade at the same time, resulting in a deadlock:
    // https://github.com/Amanieu/parking_lot/issues/200
    // We face no such issue because we only provide try_upgrade().

    /// Try to upgrade a shared lock to an exclusive lock.
    ///
    /// # Panics
    /// Panics if the lock is not locked shared.
    pub fn try_upgrade(&self) -> bool {
        let current = self.0.load(SeqCst);
        assert!(current >= READER);
        let num_readers = current >> 1;
        if num_readers > 1 {
            return false;
        }
        self.0
            .compare_exchange_weak(current, WRITER, SeqCst, SeqCst)
            .is_ok()
    }

    /// Unlock a shared lock.
    ///
    /// # Panics
    /// Panics if the lock is not locked shared.
    pub fn unlock_shared(&self) {
        let prev = self.0.fetch_sub(READER, SeqCst);
        assert_eq!(prev & WRITER, 0);
        assert!((prev >> 1) != 0);
    }

    /// Unlock an exclusive lock.
    ///
    /// # Panics
    /// Panics if the lock is not locked exclusive.
    pub fn unlock_exclusive(&self) {
        let prev = self.0.swap(0, SeqCst);
        assert!(prev & WRITER != 0);
    }
}
