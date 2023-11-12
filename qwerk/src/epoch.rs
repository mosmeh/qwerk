use crate::slotted_cell::{Slot, SlottedCell};
use crossbeam_utils::{Backoff, CachePadded};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering::SeqCst},
        Arc,
    },
    thread::JoinHandle,
    time::Duration,
};

pub struct EpochFramework {
    state: Arc<State>,
    epoch_bumper: Option<JoinHandle<()>>,
}

impl EpochFramework {
    pub fn with_epoch_duration(epoch_duration: Duration) -> Self {
        // >= 2 to avoid overflow in EpochGuard::reclamation_epoch()
        const INITIAL_EPOCH: u64 = 2;
        let state = Arc::new(State {
            global_epoch: INITIAL_EPOCH.into(),
            local_epochs: Default::default(),
            is_running: true.into(),
        });
        let epoch_bumper = {
            let state = state.clone();
            std::thread::spawn(move || {
                let mut global_epoch = INITIAL_EPOCH;
                while state.is_running.load(SeqCst) {
                    for local_epoch in state.local_epochs.iter() {
                        let backoff = Backoff::new();
                        while local_epoch.load(SeqCst) < global_epoch {
                            backoff.snooze();
                        }
                    }
                    global_epoch = state.global_epoch.fetch_add(1, SeqCst) + 1;
                    std::thread::sleep(epoch_duration);
                }
            })
        };
        Self {
            state,
            epoch_bumper: Some(epoch_bumper),
        }
    }

    pub fn acquire(&self) -> EpochGuard {
        let guard = EpochGuard {
            global_epoch: &self.state.global_epoch,
            local_epoch: self.state.local_epochs.alloc_slot(),
        };
        guard.refresh();
        guard
    }
}

impl Default for EpochFramework {
    fn default() -> Self {
        Self::with_epoch_duration(Duration::from_millis(40))
    }
}

impl Drop for EpochFramework {
    fn drop(&mut self) {
        self.state.is_running.store(false, SeqCst);
        let _ = std::mem::take(&mut self.epoch_bumper).unwrap().join();
    }
}

struct State {
    global_epoch: AtomicU64,
    local_epochs: SlottedCell<CachePadded<AtomicU64>>,
    is_running: AtomicBool,
}

pub struct EpochGuard<'a> {
    global_epoch: &'a AtomicU64,
    local_epoch: Slot<'a, CachePadded<AtomicU64>>,
}

impl EpochGuard<'_> {
    /// Takes a snapshot of the current global epoch.
    ///
    /// Returns the current global epoch.
    pub fn refresh(&self) -> u64 {
        let epoch = self.global_epoch.load(SeqCst);
        self.local_epoch.store(epoch, SeqCst);
        epoch
    }
}

impl Drop for EpochGuard<'_> {
    fn drop(&mut self) {
        self.local_epoch.store(u64::MAX, SeqCst);
    }
}
