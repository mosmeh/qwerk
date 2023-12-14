// Epoch framework is described in:
// Tu et al. 2013. Speedy transactions in multicore in-memory databases. https://doi.org/10.1145/2517349.2522713
// Chandramouli et al. 2018. FASTER: A Concurrent Key-Value Store with In-Place Updates. https://doi.org/10.1145/3183713.3196898
// Li et al. 2022. Performant Almost-Latch-Free Data Structures Using Epoch Protection. https://doi.org/10.1145/3533737.3535091

use crate::slotted_cell::{Slot, SlottedCell};
use crossbeam_utils::{Backoff, CachePadded};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering::SeqCst},
        Arc,
    },
    thread::JoinHandle,
    time::Duration,
};

// All local_epochs are either global_epoch or global_epoch - 1.
// Thus global_epoch - 2 is the reclamation epoch.
const RECLAMATION_EPOCH_OFFSET: u32 = 2;

const OFFLINE_EPOCH: u32 = u32::MAX;

/// A unit of time for concurrency control and durability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Epoch(pub u32);

impl Epoch {
    pub const ZERO: Self = Self(0);

    pub const fn increment(self) -> Self {
        Self(self.0 + 1)
    }
}

pub struct Config {
    pub initial_epoch: Epoch,
    pub epoch_duration: Duration,
}

pub struct EpochFramework {
    shared: Arc<SharedState>,
    epoch_bumper: Option<JoinHandle<()>>,
}

impl EpochFramework {
    pub fn new(config: Config) -> Self {
        // Ensure that reclamation_epoch > 0
        let initial_epoch = config
            .initial_epoch
            .max(Epoch(RECLAMATION_EPOCH_OFFSET + 1));

        let shared = Arc::new(SharedState {
            global_epoch: initial_epoch.0.into(),
            local_epochs: Default::default(),
            is_running: true.into(),
        });
        let epoch_bumper = {
            let shared = shared.clone();
            std::thread::Builder::new()
                .name("epoch_bumper".into())
                .spawn(move || {
                    let mut global_epoch = initial_epoch.0;
                    while shared.is_running.load(SeqCst) {
                        for local_epoch in shared.local_epochs.iter() {
                            let backoff = Backoff::new();
                            while local_epoch.load(SeqCst) < global_epoch {
                                backoff.snooze();
                            }
                        }
                        global_epoch = shared.global_epoch.fetch_add(1, SeqCst) + 1;
                        std::thread::sleep(config.epoch_duration);
                    }
                })
                .unwrap()
        };

        Self {
            shared,
            epoch_bumper: Some(epoch_bumper),
        }
    }

    pub fn acquire(&self) -> EpochGuard {
        EpochGuard {
            global_epoch: &self.shared.global_epoch,
            local_epoch: self
                .shared
                .local_epochs
                .alloc_with(|_| AtomicU32::new(OFFLINE_EPOCH).into()),
        }
    }
}

impl Drop for EpochFramework {
    fn drop(&mut self) {
        self.shared.is_running.store(false, SeqCst);
        let _ = std::mem::take(&mut self.epoch_bumper).unwrap().join();
    }
}

struct SharedState {
    global_epoch: AtomicU32,
    local_epochs: SlottedCell<CachePadded<AtomicU32>>,
    is_running: AtomicBool,
}

pub struct EpochGuard<'a> {
    global_epoch: &'a AtomicU32,
    local_epoch: Slot<'a, CachePadded<AtomicU32>>,
}

impl EpochGuard<'_> {
    /// Takes a snapshot of the current global epoch.
    ///
    /// Returns the current global epoch.
    pub fn refresh(&self) -> Epoch {
        let epoch = self.global_epoch.load(SeqCst);
        self.local_epoch.store(epoch, SeqCst);
        Epoch(epoch)
    }

    /// Temporarily marks the owner of this guard as not participating in the
    /// epoch framework.
    ///
    /// Until the next call to [`refresh`], the owner of this guard is not
    /// considered as a participant of the epoch framework.
    ///
    /// [`refresh`]: #method.refresh
    pub fn release(&self) {
        self.local_epoch.store(OFFLINE_EPOCH, SeqCst);
    }

    /// Returns the reclamation epoch.
    ///
    /// The reclamation epoch is the largest epoch that no participant of the
    /// epoch framework is currently in.
    pub fn reclamation_epoch(&self) -> Epoch {
        Epoch(self.global_epoch.load(SeqCst) - RECLAMATION_EPOCH_OFFSET)
    }
}

impl Drop for EpochGuard<'_> {
    fn drop(&mut self) {
        self.release();
    }
}
