use crossbeam_utils::Backoff;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

// bit [0]:     whether exclusive lock is held
// bits[1..63]: number of shared locks held

const WRITER: u64 = 0x1;
const READER: u64 = 0x2;
const READER_FULL: u64 = !WRITER;

/// A reader-writer lock.
pub struct Lock(AtomicU64);

impl Lock {
    pub const fn new_unlocked() -> Self {
        Self(AtomicU64::new(0))
    }

    pub const fn new_locked_exclusive() -> Self {
        Self(AtomicU64::new(WRITER))
    }

    pub fn is_locked(&self) -> bool {
        self.0.load(SeqCst) != 0
    }

    pub fn is_locked_exclusive(&self) -> bool {
        self.0.load(SeqCst) & WRITER != 0
    }

    #[must_use]
    pub fn read(&self) -> ReadGuard {
        let backoff = Backoff::new();
        while !self.try_lock_shared() {
            backoff.snooze();
        }
        ReadGuard(self)
    }

    #[must_use]
    pub fn write(&self) -> WriteGuard {
        let backoff = Backoff::new();
        while !self.try_lock_exclusive() {
            backoff.snooze();
        }
        WriteGuard(self)
    }

    #[must_use]
    pub fn try_read(&self) -> Option<ReadGuard> {
        // We shouldn't use then_some(ReadGuard(self)) here because
        // we don't want to drop() the guard if we fail to acquire the lock.
        self.try_lock_shared().then(|| ReadGuard(self))
    }

    #[must_use]
    pub fn try_write(&self) -> Option<WriteGuard> {
        self.try_lock_exclusive().then(|| WriteGuard(self))
    }

    // parking_lot doesn't allow upgrading read lock to write lock, and only
    // "upgradable read" locks can be upgraded. This is to avoid two read locks
    // from trying to upgrade at the same time, resulting in a deadlock:
    // https://github.com/Amanieu/parking_lot/issues/200
    // We face no such issue because we only provide try_upgrade() and not
    // upgrade().

    /// Try to upgrade a read lock to a write lock.
    ///
    /// # Panics
    /// Panics if the lock is not read-locked.
    #[must_use]
    pub fn try_upgrade(&self) -> Option<WriteGuard> {
        let current = self.0.load(SeqCst);
        assert_eq!(current & WRITER, 0, "lock is write-locked");
        assert!(current >= READER, "lock is not read-locked");
        let num_readers = current >> 1;
        if num_readers > 1 {
            return None;
        }
        self.0
            .compare_exchange_weak(current, WRITER, SeqCst, SeqCst)
            .is_ok()
            .then(|| WriteGuard(self))
    }

    /// Forcibly unlock a read lock.
    ///
    /// # Panics
    /// Panics if the lock is not read-locked.
    pub fn force_unlock_read(&self) {
        let prev = self.0.fetch_sub(READER, SeqCst);
        assert_eq!(prev & WRITER, 0, "lock is write-locked");
        assert!(prev >= READER, "lock is not read-locked");
    }

    /// Forcibly unlock a write lock.
    ///
    /// # Panics
    /// Panics if the lock is not write-locked.
    pub fn force_unlock_write(&self) {
        let prev = self.0.swap(0, SeqCst);
        assert_eq!(prev, WRITER, "lock is not write-locked");
    }

    fn try_lock_shared(&self) -> bool {
        let current = self.0.load(SeqCst);
        if current & WRITER != 0 || current == READER_FULL {
            return false;
        }
        self.0
            .compare_exchange_weak(current, current + READER, SeqCst, SeqCst)
            .is_ok()
    }

    fn try_lock_exclusive(&self) -> bool {
        if self.0.load(SeqCst) != 0 {
            return false;
        }
        self.0
            .compare_exchange_weak(0, WRITER, SeqCst, SeqCst)
            .is_ok()
    }
}

pub struct ReadGuard<'a>(&'a Lock);

impl Drop for ReadGuard<'_> {
    fn drop(&mut self) {
        self.0.force_unlock_read();
    }
}

pub struct WriteGuard<'a>(&'a Lock);

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        self.0.force_unlock_write();
    }
}

#[cfg(test)]
mod tests {
    use super::Lock;

    #[test]
    fn shared() {
        let lock = Lock::new_unlocked();
        assert!(!lock.is_locked());
        assert!(!lock.is_locked_exclusive());
        {
            let _read_guard = lock.read();
            assert!(lock.is_locked());
            assert!(!lock.is_locked_exclusive());
            assert!(lock.try_read().is_some());
            assert!(lock.try_write().is_none());
        }
        {
            std::mem::forget(lock.read());
            assert!(lock.is_locked());
            assert!(!lock.is_locked_exclusive());
            lock.force_unlock_read();
        }
    }

    #[test]
    fn exclusive() {
        let lock = Lock::new_unlocked();
        {
            let _write_guard = lock.write();
            assert!(lock.is_locked());
            assert!(lock.is_locked_exclusive());
            assert!(lock.try_read().is_none());
            assert!(lock.try_write().is_none());
        }
        {
            std::mem::forget(lock.write());
            assert!(lock.is_locked());
            assert!(lock.is_locked_exclusive());
            lock.force_unlock_write();
        }
    }

    #[test]
    fn upgrade() {
        let lock = Lock::new_unlocked();
        std::mem::forget(lock.read());
        {
            let _write_guard = lock.try_upgrade().unwrap();
            assert!(lock.is_locked_exclusive());
            assert!(lock.try_read().is_none());
        }
        assert!(!lock.is_locked());
    }

    #[test]
    #[should_panic = "lock is not read-locked"]
    fn force_unlock_read_not_read_locked() {
        let lock = Lock::new_unlocked();
        lock.force_unlock_read();
    }

    #[test]
    #[should_panic = "lock is not write-locked"]
    fn force_unlock_write_not_write_locked() {
        let lock = Lock::new_unlocked();
        lock.force_unlock_write();
    }

    #[test]
    #[should_panic = "lock is not read-locked"]
    fn upgrade_not_read_locked() {
        let lock = Lock::new_unlocked();
        lock.try_upgrade().unwrap();
    }
}
