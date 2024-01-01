// Epoch framework is described in:
// Tu et al. 2013. Speedy transactions in multicore in-memory databases. https://doi.org/10.1145/2517349.2522713
// Chandramouli et al. 2018. FASTER: A Concurrent Key-Value Store with In-Place Updates. https://doi.org/10.1145/3183713.3196898
// Li et al. 2022. Performant Almost-Latch-Free Data Structures Using Epoch Protection. https://doi.org/10.1145/3533737.3535091

use crate::{
    signal_channel,
    slotted_cell::{Slot, SlottedCell},
};
use crossbeam_utils::{Backoff, CachePadded};
use std::{
    fmt::{Display, Formatter},
    str::FromStr,
    sync::{
        atomic::{AtomicU32, Ordering::SeqCst},
        Arc,
    },
    thread::JoinHandle,
    time::Duration,
};

// All local_epochs are either global_epoch or global_epoch - 1.
// Thus global_epoch - 2 is the reclamation epoch.
const RECLAMATION_EPOCH_OFFSET: u32 = 2;

const RELEASED_MARKER: u32 = u32::MAX;

/// A unit of time for concurrency control and durability.
///
/// Epochs are represented as integers that are non-decreasing over time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Epoch(pub u32);

impl Display for Epoch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for Epoch {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

impl Epoch {
    #[must_use]
    pub(crate) fn increment(self) -> Self {
        Self(self.0.checked_add(1).unwrap())
    }

    #[must_use]
    pub(crate) fn decrement(self) -> Self {
        Self(self.0.checked_sub(1).unwrap())
    }
}

pub struct EpochFramework {
    epoch_duration: Duration,
    shared: Arc<SharedState>,
    epoch_bumper: Option<JoinHandle<()>>,
    stop_tx: signal_channel::Sender,
}

impl EpochFramework {
    pub fn new(initial_epoch: Epoch, epoch_duration: Duration) -> Self {
        // Ensure that reclamation_epoch > 0
        let initial_epoch = initial_epoch.max(Epoch(RECLAMATION_EPOCH_OFFSET + 1));

        let shared = Arc::new(SharedState {
            global_epoch: initial_epoch.0.into(),
            local_epochs: Default::default(),
        });
        let (stop_tx, stop_rx) = signal_channel::channel();
        let epoch_bumper = {
            let shared = shared.clone();
            std::thread::Builder::new()
                .name("epoch_bumper".into())
                .spawn(move || {
                    let mut global_epoch = initial_epoch.0;
                    for _ in stop_rx.tick(epoch_duration) {
                        for local_epoch in shared.local_epochs.iter() {
                            let backoff = Backoff::new();
                            while local_epoch.load(SeqCst) < global_epoch {
                                backoff.snooze();
                            }
                        }
                        global_epoch = shared.global_epoch.fetch_add(1, SeqCst) + 1;
                    }
                })
                .unwrap()
        };

        Self {
            epoch_duration,
            shared,
            epoch_bumper: Some(epoch_bumper),
            stop_tx,
        }
    }

    pub const fn epoch_duration(&self) -> Duration {
        self.epoch_duration
    }

    pub fn global_epoch(&self) -> Epoch {
        Epoch(self.shared.global_epoch.load(SeqCst))
    }

    pub fn participant(&self) -> EpochParticipant {
        EpochParticipant {
            global_epoch: &self.shared.global_epoch,
            local_epoch: self
                .shared
                .local_epochs
                .alloc_with(|_| AtomicU32::new(RELEASED_MARKER).into()),
        }
    }

    /// Waits until the global epoch is incremented.
    ///
    /// Returns the new global epoch.
    pub fn sync(&self) -> Epoch {
        let new_global_epoch = self.shared.global_epoch.load(SeqCst) + 1;
        let backoff = Backoff::new();
        while self.shared.global_epoch.load(SeqCst) < new_global_epoch {
            backoff.snooze();
        }
        Epoch(new_global_epoch)
    }
}

impl Drop for EpochFramework {
    fn drop(&mut self) {
        self.stop_tx.send();
        let _ = self.epoch_bumper.take().unwrap().join();
    }
}

struct SharedState {
    global_epoch: AtomicU32,
    local_epochs: SlottedCell<CachePadded<AtomicU32>>,
}

pub struct EpochParticipant<'a> {
    global_epoch: &'a AtomicU32,
    local_epoch: Slot<'a, CachePadded<AtomicU32>>,
}

impl<'a> EpochParticipant<'a> {
    #[must_use]
    pub fn acquire(&mut self) -> EpochProtector<'a, '_> {
        let (_, newly_acquired) = self.refresh();
        assert!(newly_acquired, "epoch protection is already acquired");
        EpochProtector { participant: self }
    }

    /// Forcibly removes the epoch protection.
    ///
    /// # Panics
    ///
    /// Panics if the epoch protection is already released.
    pub fn force_release(&self) {
        let prev = self.local_epoch.swap(RELEASED_MARKER, SeqCst);
        assert_ne!(
            prev, RELEASED_MARKER,
            "epoch protection is already released"
        );
    }

    /// Forcibly moves the local epoch forward to the current global epoch.
    ///
    /// Returns the current global epoch.
    ///
    /// # Panics
    ///
    /// Panics if the epoch protection is not acquired.
    pub fn force_refresh(&self) -> Epoch {
        let (epoch, newly_acquired) = self.refresh();
        assert!(!newly_acquired, "epoch protection is not acquired");
        epoch
    }

    /// Moves the local epoch forward to the current global epoch.
    ///
    /// Returns the current global epoch and whether this participant
    /// newly acquired the epoch protection due to this call.
    fn refresh(&self) -> (Epoch, bool) {
        let epoch = self.global_epoch.load(SeqCst);
        let prev = self.local_epoch.swap(epoch, SeqCst);
        (Epoch(epoch), prev == RELEASED_MARKER)
    }

    /// Returns the reclamation epoch.
    ///
    /// The reclamation epoch is the largest epoch that no participant of the
    /// epoch framework is currently in.
    pub fn reclamation_epoch(&self) -> Epoch {
        Epoch(self.global_epoch.load(SeqCst) - RECLAMATION_EPOCH_OFFSET)
    }
}

pub struct EpochProtector<'fw, 'participant> {
    participant: &'participant mut EpochParticipant<'fw>,
}

impl Drop for EpochProtector<'_, '_> {
    fn drop(&mut self) {
        self.participant.force_release();
    }
}

#[cfg(test)]
mod tests {
    use super::{Epoch, EpochFramework, EpochParticipant};
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering::SeqCst},
            Arc, Barrier,
        },
        time::Duration,
    };

    fn new_epoch_fw() -> EpochFramework {
        EpochFramework::new(Epoch(0), Duration::from_millis(1))
    }

    #[test]
    fn sync() {
        let epoch_fw = new_epoch_fw();
        let epoch1 = epoch_fw.sync();
        let epoch2 = epoch_fw.sync();
        assert!(epoch1 < epoch2);
    }

    #[test]
    fn epoch_protector() {
        let epoch_fw = Arc::new(new_epoch_fw());

        let synced = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(2));
        let thread = {
            let epoch_fw = epoch_fw.clone();
            let synced = synced.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let mut participant = epoch_fw.participant();
                std::mem::forget(participant.acquire());
                barrier.wait();
                while !synced.load(SeqCst) {
                    participant.force_refresh();
                }
                participant.force_release();
            })
        };

        barrier.wait();
        epoch_fw.sync();
        epoch_fw.sync();
        synced.store(true, SeqCst);
        thread.join().unwrap();
        epoch_fw.sync();
        epoch_fw.sync();
    }

    #[test]
    #[should_panic = "epoch protection is not acquired"]
    fn force_refresh_without_acquire() {
        struct PanicGuard<'a>(&'a EpochParticipant<'a>);

        impl Drop for PanicGuard<'_> {
            fn drop(&mut self) {
                self.0.force_release();
            }
        }

        let epoch_fw = new_epoch_fw();
        let participant = epoch_fw.participant();
        let _guard = PanicGuard(&participant);
        participant.force_refresh();
    }

    #[test]
    #[should_panic = "epoch protection is already released"]
    fn force_release_without_acquire() {
        let epoch_fw = new_epoch_fw();
        let participant = epoch_fw.participant();
        participant.force_release();
    }
}
