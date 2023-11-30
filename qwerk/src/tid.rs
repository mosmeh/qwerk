use crate::Epoch;

// TID format:
// bits[63:32] - epoch
// bits[31:2]  - sequence (distinguishes transactions within the same epoch)
// bits[1:0]   - flags (concurrency control protocol-specific)

/// Transaction ID and flag bits.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Tid(pub u64);

impl Tid {
    pub const ZERO: Self = Self(0);
    const EPOCH_SHIFT: u32 = 32;
    const SEQUENCE_SHIFT: u32 = 2;
    const FLAGS: u64 = (1 << Self::SEQUENCE_SHIFT) - 1;

    pub const fn epoch(self) -> Epoch {
        Epoch((self.0 >> Self::EPOCH_SHIFT) as u32)
    }

    const fn from_epoch_and_sequence(epoch: Epoch, sequence: u32) -> Self {
        Self(((epoch.0 as u64) << Self::EPOCH_SHIFT) | ((sequence as u64) << Self::SEQUENCE_SHIFT))
    }

    const fn increment_sequence(self) -> Self {
        Self(self.0 + (1 << Self::SEQUENCE_SHIFT))
    }

    const fn has_flags(self) -> bool {
        self.0 & Self::FLAGS != 0
    }

    const fn without_flags(self) -> Self {
        Self(self.0 & !Self::FLAGS)
    }
}

/// A generator of Silo-style TIDs.
pub struct TidGenerator {
    max_tid: Tid,
}

impl Default for TidGenerator {
    fn default() -> Self {
        Self { max_tid: Tid::ZERO }
    }
}

impl TidGenerator {
    pub fn begin_transaction(&mut self) -> TidRwSet {
        // The new TID must be:
        // (b) larger than the worker’s most recently chosen TID
        let max_tid = self.max_tid;
        TidRwSet {
            generator: self,
            max_tid,
        }
    }
}

/// A set of TIDs read or written by a transaction.
pub struct TidRwSet<'a> {
    generator: &'a mut TidGenerator,
    max_tid: Tid,
}

impl TidRwSet<'_> {
    pub fn add(&mut self, tid: Tid) {
        // The new TID must be:
        // (a) larger than the TID of any record read or written
        //     by the transaction
        self.max_tid = self.max_tid.max(tid.without_flags());
    }

    pub fn generate_tid(self, epoch: Epoch) -> Tid {
        let epoch_of_max_tid = self.max_tid.epoch();
        assert!(epoch_of_max_tid <= epoch);

        // The new TID must be:
        // (c) in the current global epoch
        let mut new_tid = if epoch_of_max_tid == epoch {
            self.max_tid
        } else {
            Tid::from_epoch_and_sequence(epoch, 0)
        };
        new_tid = new_tid.increment_sequence();

        assert!(!new_tid.has_flags());
        self.generator.max_tid = new_tid;
        new_tid
    }
}
