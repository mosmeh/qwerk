use crossbeam_utils::Backoff;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

// bit [0]:     Whether this lock is dead. If this bit is set, the lock
//              is considered to be locked forever.
// bit [1]:     Whether exclusive lock is held
// bits[2..63]: Number of shared locks held

const DEAD_SHIFT: u64 = 0;
const WRITER_SHIFT: u64 = 1;
const READER_SHIFT: u64 = 2;

const DEAD: u64 = 1 << DEAD_SHIFT;
const WRITER: u64 = 1 << WRITER_SHIFT;
const READER: u64 = 1 << READER_SHIFT;

const READER_FULL: u64 = !(READER - 1);

/// A reader-writer lock.
///
/// This lock is similar to [`RwLock`], but it allows upgrading a read lock to
/// a write lock.
///
/// This lock also has a "dead" state, which is used to indicate that the lock
/// is permanently locked. This is used to prevent a record from being accessed
/// after it has been removed.
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

    /// Acquire a read lock.
    ///
    /// This method will block until the lock is acquired. If the lock is dead,
    /// this method will return `None`.
    #[must_use]
    pub fn read(&self) -> Option<ReadGuard> {
        let backoff = Backoff::new();
        loop {
            match self.try_lock_shared() {
                TryLockResult::Locked => return Some(ReadGuard(self)),
                TryLockResult::WouldBlock => backoff.snooze(),
                TryLockResult::Dead => return None,
            }
        }
    }

    /// Acquire a write lock.
    ///
    /// This method will block until the lock is acquired. If the lock is dead,
    /// this method will return `None`.
    #[must_use]
    pub fn write(&self) -> Option<WriteGuard> {
        let backoff = Backoff::new();
        loop {
            match self.try_lock_exclusive() {
                TryLockResult::Locked => return Some(WriteGuard(self)),
                TryLockResult::WouldBlock => backoff.snooze(),
                TryLockResult::Dead => return None,
            }
        }
    }

    /// Try to acquire a read lock.
    ///
    /// If the lock can't be immediately acquired or is dead, this method will
    /// return `None`.
    #[must_use]
    pub fn try_read(&self) -> Option<ReadGuard> {
        // We shouldn't use then_some(ReadGuard(self)) here because
        // we don't want to drop() the guard if we fail to acquire the lock.
        self.try_lock_shared().is_locked().then(|| ReadGuard(self))
    }

    /// Try to acquire a write lock.
    ///
    /// If the lock can't be immediately acquired or is dead, this method will
    /// return `None`.
    #[must_use]
    pub fn try_write(&self) -> Option<WriteGuard> {
        self.try_lock_exclusive()
            .is_locked()
            .then(|| WriteGuard(self))
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
    /// Panics if the lock is dead or not read-locked.
    #[must_use]
    pub fn try_upgrade(&self) -> Option<WriteGuard> {
        let current = self.0.load(SeqCst);
        assert_eq!(current & DEAD, 0, "lock is dead");
        assert_eq!(current & WRITER, 0, "lock is write-locked");
        assert!(current >= READER, "lock is not read-locked");
        let num_readers = current >> READER_SHIFT;
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
    /// Panics if the lock is dead or not read-locked.
    pub fn force_unlock_read(&self) {
        let prev = self.0.fetch_sub(READER, SeqCst);
        assert_eq!(prev & DEAD, 0, "lock is dead");
        assert_eq!(prev & WRITER, 0, "lock is write-locked");
        assert!(prev >= READER, "lock is not read-locked");
    }

    /// Forcibly unlock a write lock.
    ///
    /// # Panics
    /// Panics if the lock is dead or not write-locked.
    pub fn force_unlock_write(&self) {
        let prev = self.0.swap(0, SeqCst);
        assert_eq!(prev & DEAD, 0, "lock is dead");
        assert_eq!(prev, WRITER, "lock is not write-locked");
    }

    /// Kill the lock.
    ///
    /// The lock will enter a "dead" state, where it is considered to be
    /// permanently locked.
    ///
    /// # Panics
    /// Panics if the lock is already dead or not write-locked.
    pub fn kill(&self) {
        let prev = self.0.swap(DEAD | WRITER, SeqCst);
        assert_eq!(prev & DEAD, 0, "lock is already dead");
        assert_ne!(prev & WRITER, 0, "lock is not write-locked");
    }

    #[must_use]
    fn try_lock_shared(&self) -> TryLockResult {
        let current = self.0.load(SeqCst);
        if current & DEAD != 0 {
            return TryLockResult::Dead;
        }
        if current & WRITER != 0 || current == READER_FULL {
            return TryLockResult::WouldBlock;
        }
        match self
            .0
            .compare_exchange_weak(current, current + READER, SeqCst, SeqCst)
        {
            Ok(_) => TryLockResult::Locked,
            Err(_) => TryLockResult::WouldBlock,
        }
    }

    #[must_use]
    fn try_lock_exclusive(&self) -> TryLockResult {
        let current = self.0.load(SeqCst);
        if current & DEAD != 0 {
            return TryLockResult::Dead;
        }
        if current != 0 {
            return TryLockResult::WouldBlock;
        }
        match self.0.compare_exchange_weak(0, WRITER, SeqCst, SeqCst) {
            Ok(_) => TryLockResult::Locked,
            Err(_) => TryLockResult::WouldBlock,
        }
    }
}

enum TryLockResult {
    /// The lock was acquired.
    Locked,

    /// The lock was not acquired because it is already locked.
    WouldBlock,

    /// The lock was not acquired because it is dead.
    Dead,
}

impl TryLockResult {
    fn is_locked(&self) -> bool {
        matches!(self, Self::Locked)
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

impl WriteGuard<'_> {
    /// See [`Lock::kill`].
    pub fn kill(self) {
        self.0.kill();
        std::mem::forget(self); // Can't unlock a dead lock.
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
        std::mem::forget(lock.read().unwrap());
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

    #[test]
    fn try_read_dead() {
        let lock = Lock::new_locked_exclusive();
        lock.kill();
        assert!(lock.try_read().is_none());
    }

    #[test]
    fn read_dead() {
        let lock = Lock::new_locked_exclusive();
        lock.kill();
        assert!(lock.read().is_none());
    }

    #[test]
    fn try_write_dead() {
        let lock = Lock::new_locked_exclusive();
        lock.kill();
        assert!(lock.try_write().is_none());
    }

    #[test]
    fn write_dead() {
        let lock = Lock::new_locked_exclusive();
        lock.kill();
        assert!(lock.write().is_none());
    }

    #[test]
    fn kill_write_guard() {
        let lock = Lock::new_unlocked();
        lock.write().unwrap().kill();
    }

    #[test]
    #[should_panic = "lock is not write-locked"]
    fn kill_not_write_locked() {
        let lock = Lock::new_unlocked();
        lock.kill();
    }

    #[test]
    #[should_panic = "lock is already dead"]
    fn kill_already_dead() {
        let lock = Lock::new_locked_exclusive();
        lock.kill();
        lock.kill();
    }

    #[test]
    #[should_panic = "lock is dead"]
    fn unlock_read_dead() {
        let lock = Lock::new_locked_exclusive();
        lock.kill();
        lock.force_unlock_read();
    }

    #[test]
    #[should_panic = "lock is dead"]
    fn unlock_write_dead() {
        let lock = Lock::new_locked_exclusive();
        lock.kill();
        lock.force_unlock_write();
    }
}
