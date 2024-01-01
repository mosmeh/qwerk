// QSBR is described in:
// Hart et al. 2006. Making lockless synchronization fast: performance implications of memory reclamation. https://ieeexplore.ieee.org/document/1639261

use crate::{
    loom::sync::atomic::{AtomicU64, Ordering::SeqCst},
    slotted_cell::{Slot, SlottedCell},
};
use crossbeam_utils::{Backoff, CachePadded};

const OFFLINE_MARKER: u64 = u64::MAX;

/// Quiescent state-based memory reclamation.
pub struct MemoryReclamation {
    global_counter: AtomicU64,
    local_counters: SlottedCell<CachePadded<AtomicU64>>,
    threshold: usize,
}

impl MemoryReclamation {
    /// Creates a new `MemoryReclamation` with the given threshold.
    ///
    /// The threshold is the number of bytes that will be accumulated before
    /// [`Reclaimer`]s try to reclaim them.
    pub fn new(threshold: usize) -> Self {
        Self {
            global_counter: Default::default(),
            local_counters: Default::default(),
            threshold,
        }
    }

    pub fn reclaimer<T>(&self) -> Reclaimer<T> {
        Reclaimer {
            global: self,
            local_counter: self
                .local_counters
                .alloc_with(|_| AtomicU64::new(OFFLINE_MARKER).into()),
            debt: 0,
            sized: Default::default(),
            bytes: Default::default(),
        }
    }

    /// Waits until all active [`Reclaimer`]s experience quiescent state.
    fn sync(&self) {
        let counter = self.global_counter.fetch_add(1, SeqCst) + 1;
        for local_counter in self.local_counters.iter() {
            let backoff = Backoff::new();
            while local_counter.load(SeqCst) < counter {
                backoff.snooze();
            }
        }
    }
}

pub struct Reclaimer<'a, T> {
    global: &'a MemoryReclamation,
    local_counter: Slot<'a, CachePadded<AtomicU64>>,
    debt: usize,
    sized: Vec<Box<T>>,
    bytes: Vec<Box<[u8]>>,
}

impl<'a, T> Reclaimer<'a, T> {
    /// Enters non-quiescent state.
    ///
    /// Returns an [`EnterGuard`] that will make the reclaimer return to
    /// quiescent state when dropped.
    #[must_use]
    pub fn enter(&mut self) -> EnterGuard<'a, '_, T> {
        let was_quiescent = self.quiesce_and_enter();
        assert!(was_quiescent, "already in non-quiescent state");
        EnterGuard { reclaimer: self }
    }

    /// Declares that the owner of this reclaimer momentarily experienced
    /// quiescent state and makes the reclaimer enter non-quiescent state.
    ///
    /// Returns `true` if the reclaimer was in quiescent state before this
    /// call.
    fn quiesce_and_enter(&self) -> bool {
        let counter = self.global.global_counter.load(SeqCst);
        let prev = self.local_counter.swap(counter, SeqCst);
        prev == OFFLINE_MARKER
    }

    /// Forces the reclaimer to leave non-quiescent state.
    ///
    /// This is useful when combined with [`std::mem::forget`] to stay in
    /// non-quiescent state without maintaining an [`EnterGuard`].
    ///
    /// # Panics
    ///
    /// Panics if the reclaimer is already in quiescent state.
    pub fn force_leave(&self) {
        let prev_counter = self.local_counter.swap(OFFLINE_MARKER, SeqCst);
        assert_ne!(prev_counter, OFFLINE_MARKER, "already in quiescent state");
    }

    pub fn defer_drop(&mut self, x: Box<T>) {
        self.debt += std::mem::size_of::<T>();
        self.sized.push(x);
    }

    pub fn defer_drop_bytes(&mut self, bytes: Box<[u8]>) {
        self.debt += bytes.len();
        self.bytes.push(bytes);
    }

    pub fn reclaim(&mut self) {
        if self.debt >= self.global.threshold {
            self.force_reclaim();
        }
    }

    fn force_reclaim(&mut self) {
        if self.sized.is_empty() && self.bytes.is_empty() {
            assert_eq!(self.debt, 0);
            return;
        }
        self.global.sync();
        self.sized.clear();
        self.bytes.clear();
        self.debt = 0;
    }
}

impl<T> Drop for Reclaimer<'_, T> {
    fn drop(&mut self) {
        self.force_reclaim();
    }
}

pub struct EnterGuard<'global, 'reclaimer, T> {
    reclaimer: &'reclaimer mut Reclaimer<'global, T>,
}

impl<T> EnterGuard<'_, '_, T> {
    /// Declares that the owner of this guard momentarily experienced quiescent
    /// state.
    ///
    /// This is equivalent to dropping the guard and reacquiring it.
    pub fn quiesce(&self) {
        let was_quiescent = self.reclaimer.quiesce_and_enter();
        assert!(!was_quiescent);
    }

    pub fn defer_drop(&mut self, x: Box<T>) {
        self.reclaimer.defer_drop(x);
    }

    pub fn defer_drop_bytes(&mut self, bytes: Box<[u8]>) {
        self.reclaimer.defer_drop_bytes(bytes);
    }
}

impl<T> Drop for EnterGuard<'_, '_, T> {
    fn drop(&mut self) {
        self.reclaimer.force_leave();
    }
}

#[cfg(test)]
mod tests {
    use super::MemoryReclamation;
    use crate::loom::sync::{
        atomic::{AtomicBool, Ordering::SeqCst},
        Arc, Barrier,
    };

    #[test]
    fn threshold() {
        let dropped = Arc::new(AtomicBool::new(false));
        let reclamation = MemoryReclamation::new(1024);
        let mut reclaimer = reclamation.reclaimer();

        assert!(std::mem::size_of::<NeedsDrop>() < 1024);
        reclaimer.defer_drop(Box::new(NeedsDrop(dropped.clone())));

        reclaimer.reclaim();
        assert!(!dropped.load(SeqCst));

        reclaimer.defer_drop_bytes(vec![0; 1024].into_boxed_slice());
        assert!(!dropped.load(SeqCst));

        reclaimer.reclaim();
        assert!(dropped.load(SeqCst));
    }

    #[test]
    fn enter_and_leave() {
        let dropped = Arc::new(AtomicBool::new(false));
        let reclamation = MemoryReclamation::new(0);

        let mut reclaimer = reclamation.reclaimer();
        reclaimer.defer_drop(Box::new(NeedsDrop(dropped.clone())));

        let mut reclaimer2 = reclamation.reclaimer::<()>();
        drop(reclaimer2.enter());

        reclaimer.reclaim();
        assert!(dropped.load(SeqCst));
    }

    #[test]
    fn quiesce() {
        let reclamation = Arc::new(MemoryReclamation::new(0));

        let dropped = Arc::new(AtomicBool::new(false));
        let mut reclaimer = reclamation.reclaimer();
        reclaimer.defer_drop(Box::new(NeedsDrop(dropped.clone())));

        let barrier = Arc::new(Barrier::new(2));
        let thread = {
            let reclamation = reclamation.clone();
            let dropped = dropped.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let mut reclaimer = reclamation.reclaimer::<()>();
                let guard = reclaimer.enter();
                barrier.wait();
                assert!(!dropped.load(SeqCst));
                while !dropped.load(SeqCst) {
                    guard.quiesce();
                }
            })
        };

        barrier.wait();
        reclaimer.reclaim();
        thread.join().unwrap();
        assert!(dropped.load(SeqCst));
    }

    #[test]
    fn drop_reclaimer() {
        let dropped = Arc::new(AtomicBool::new(false));
        let reclamation = MemoryReclamation::new(1024);
        let mut reclaimer = reclamation.reclaimer();

        assert!(std::mem::size_of::<NeedsDrop>() < 1024);
        reclaimer.defer_drop(Box::new(NeedsDrop(dropped.clone())));
        assert!(!dropped.load(SeqCst));

        drop(reclaimer);
        assert!(dropped.load(SeqCst));
    }

    #[test]
    #[should_panic = "already in quiescent state"]
    fn force_leave() {
        let reclamation = MemoryReclamation::new(0);
        let reclaimer = reclamation.reclaimer::<()>();
        reclaimer.force_leave();
    }

    struct NeedsDrop(Arc<AtomicBool>);

    impl Drop for NeedsDrop {
        fn drop(&mut self) {
            self.0.store(true, SeqCst);
        }
    }
}
