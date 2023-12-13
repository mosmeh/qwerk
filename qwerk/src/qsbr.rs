// QSBR is described in:
// Hart et al. 2006. Making lockless synchronization fast: performance implications of memory reclamation. https://ieeexplore.ieee.org/document/1639261

use crate::slotted_cell::{Slot, SlottedCell};
use crossbeam_utils::{Backoff, CachePadded};
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

const OFFLINE_COUNTER: u64 = u64::MAX;

/// Quiescent state-based reclamation
#[derive(Default)]
pub struct Qsbr {
    global_counter: AtomicU64,
    local_counters: SlottedCell<CachePadded<AtomicU64>>,
}

impl Qsbr {
    pub fn acquire(&self) -> QsbrGuard {
        QsbrGuard {
            qsbr: self,
            local_counter: self
                .local_counters
                .alloc_with(|_| AtomicU64::new(OFFLINE_COUNTER).into()),
        }
    }

    /// Waits until all owners of [`QsbrGuard`]s `acquire`d from this [`Qsbr`]
    /// experience quiescent states at least once.
    pub fn sync(&self) {
        let counter = self.global_counter.fetch_add(1, SeqCst) + 1;
        for local_counter in self.local_counters.iter() {
            let backoff = Backoff::new();
            while local_counter.load(SeqCst) < counter {
                backoff.snooze();
            }
        }
    }
}

/// A representation of a participant of QSBR.
///
/// While this guard is alive, the owner of this guard has to periodically
/// declare that it is on a quiescent state by calling [`quiesce`].
///
/// [`quiesce`]: #method.quiesce
pub struct QsbrGuard<'a> {
    qsbr: &'a Qsbr,
    local_counter: Slot<'a, CachePadded<AtomicU64>>,
}

impl QsbrGuard<'_> {
    /// Declares that the owner of this guard is on a quiescent state.
    pub fn quiesce(&self) {
        let counter = self.qsbr.global_counter.load(SeqCst);
        self.local_counter.store(counter, SeqCst);
    }

    /// Temporarily marks the owner of this guard as not participating in QSBR.
    ///
    /// Until the next call to [`quiesce`], the owner of this guard is not
    /// considered as a participant of QSBR.
    ///
    /// [`quiesce`]: #method.quiesce
    pub fn mark_as_offline(&self) {
        self.local_counter.store(OFFLINE_COUNTER, SeqCst);
    }

    pub fn is_online(&self) -> bool {
        self.local_counter.load(SeqCst) != OFFLINE_COUNTER
    }
}

impl Drop for QsbrGuard<'_> {
    fn drop(&mut self) {
        self.mark_as_offline()
    }
}
