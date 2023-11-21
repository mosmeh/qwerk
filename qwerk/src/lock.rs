use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

// parking_lot doesn't allow upgrading read lock to write lock, and only
// "upgradable read" locks can be upgraded. This is to avoid two read locks from
// trying to upgrade at the same time, resulting in a deadlock:
// https://github.com/Amanieu/parking_lot/issues/200
// We face no such issue because we only need try_upgrade().

const EXCLUSIVE: u64 = 0x1;
const READERS: u64 = 0x2;
const READERS_FULL: u64 = !EXCLUSIVE;

/// A reader-writer lock that does not block.
pub struct NoWaitRwLock(AtomicU64);

impl NoWaitRwLock {
    pub const fn new_locked_shared() -> Self {
        Self(AtomicU64::new(READERS))
    }

    pub const fn new_locked_exclusive() -> Self {
        Self(AtomicU64::new(EXCLUSIVE))
    }

    pub fn is_locked(&self) -> bool {
        self.0.load(SeqCst) != 0
    }

    pub fn is_locked_exclusive(&self) -> bool {
        self.0.load(SeqCst) & EXCLUSIVE != 0
    }

    pub fn try_lock_shared(&self) -> bool {
        let current = self.0.load(SeqCst);
        if current & EXCLUSIVE != 0 || current == READERS_FULL {
            return false;
        }
        self.0
            .compare_exchange_weak(current, current + READERS, SeqCst, SeqCst)
            .is_ok()
    }

    pub fn try_lock_exclusive(&self) -> bool {
        if self.0.load(SeqCst) != 0 {
            return false;
        }
        self.0
            .compare_exchange_weak(0, EXCLUSIVE, SeqCst, SeqCst)
            .is_ok()
    }

    /// Try to upgrade a shared lock to an exclusive lock.
    pub fn try_upgrade(&self) -> bool {
        let current = self.0.load(SeqCst);
        assert!(current >= READERS);
        let num_readers = current >> 1;
        if num_readers > 1 {
            return false;
        }
        self.0
            .compare_exchange_weak(current, EXCLUSIVE, SeqCst, SeqCst)
            .is_ok()
    }

    pub fn unlock_shared(&self) {
        let prev = self.0.fetch_sub(READERS, SeqCst);
        assert_eq!(prev & EXCLUSIVE, 0);
        assert!((prev >> 1) != 0);
    }

    pub fn unlock_exclusive(&self) {
        let prev = self.0.swap(0, SeqCst);
        assert!(prev & EXCLUSIVE != 0);
    }
}
