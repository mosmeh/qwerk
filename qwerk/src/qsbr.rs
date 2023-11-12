use crate::slotted_cell::{Slot, SlottedCell};
use crossbeam_utils::{Backoff, CachePadded};
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

/// Quiescent state-based reclamation
#[derive(Default)]
pub struct Qsbr {
    global_counter: AtomicU64,
    local_counters: SlottedCell<CachePadded<AtomicU64>>,
}

impl Qsbr {
    pub fn acquire(&self) -> QsbrGuard {
        let guard = QsbrGuard {
            qsbr: self,
            local_counter: self.local_counters.alloc_slot(),
        };
        guard.quiesce();
        guard
    }
}

pub struct QsbrGuard<'a> {
    qsbr: &'a Qsbr,
    local_counter: Slot<'a, CachePadded<AtomicU64>>,
}

impl QsbrGuard<'_> {
    /// Declares that the owner of this guard is on a quiescent state.
    ///
    /// Returns the current global counter.
    pub fn quiesce(&self) -> u64 {
        let counter = self.qsbr.global_counter.load(SeqCst);
        self.local_counter.store(counter, SeqCst);
        counter
    }

    /// Waits until all owners of guards `acquire`d from the same [`Qsbr`]
    /// experience quiescent states at least once.
    ///
    /// Returns a lower bound of the counter value seen by all [`QsbrGuard`]s
    /// for the same [`Qsbr`].
    pub fn sync(&self) -> u64 {
        let counter = self.qsbr.global_counter.fetch_add(1, SeqCst) + 1;
        self.local_counter.store(counter, SeqCst);

        for local_counter in self.qsbr.local_counters.iter() {
            let backoff = Backoff::new();
            while local_counter.load(SeqCst) < counter {
                self.quiesce();
                backoff.snooze();
            }
        }

        counter
    }
}

impl Drop for QsbrGuard<'_> {
    fn drop(&mut self) {
        self.local_counter.store(u64::MAX, SeqCst);
    }
}
