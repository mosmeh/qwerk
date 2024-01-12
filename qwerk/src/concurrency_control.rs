mod optimistic;
mod pessimistic;

pub use optimistic::Optimistic;
pub use pessimistic::Pessimistic;

use crate::{
    epoch::EpochFramework, memory_reclamation::Reclaimer, persistence::LogEntry, record::Record,
    small_bytes::SmallBytes, tid::Tid, Index, Result, Shared,
};

/// The default concurrency control protocol.
pub type DefaultProtocol = Optimistic;

/// Concurrency control protocol.
pub trait ConcurrencyControl: Send + Sync + Default + 'static + ConcurrencyControlInternal {}

pub trait ConcurrencyControlInternal: Send + Sync + Default + 'static {
    type Record: Record;
    type Executor<'a>: TransactionExecutor + 'a
    where
        Self: 'a;

    /// Loads a record into `index`.
    ///
    /// This is called when the database is opened and before any transactions
    /// are started.
    ///
    /// This can be called multiple times for the same `key`. Implementations
    /// should make sure a record with the highest `tid` for the `key` ends up
    /// in `index`.
    ///
    /// `value` of `None` means a tombstone.
    fn load_record(
        index: &Index<Self::Record>,
        key: SmallBytes,
        value: Option<Box<[u8]>>,
        tid: Tid,
    );

    fn executor<'a>(
        &'a self,
        index: &'a Index<Self::Record>,
        epoch_fw: &'a EpochFramework,
        reclaimer: Reclaimer<'a, Self::Record>,
    ) -> Self::Executor<'a>;
}

// Flow of a transaction (successful case):
// 1. begin_transaction
// 2. read/write
// 3. validate
// 4. log (if persistence is enabled and the transaction contains writes)
// 5. precommit
// 6. end_transaction
// 7. wait_for_durability (if enabled)

// If any of `read`, `write`, `validate` returns `Err`:
// 1. abort
// 2. end_transaction

pub trait TransactionExecutor {
    /// Called before a transaction begins.
    fn begin_transaction(&mut self) {}

    /// Called after a transaction ends.
    fn end_transaction(&mut self) {}

    /// Reads a key-value pair from the transaction.
    ///
    /// Returns `None` if the key does not exist.
    fn read(&mut self, key: &[u8]) -> Result<Option<&[u8]>>;

    /// Writes a key-value pair into the transaction.
    ///
    /// `value` of `None` means the key is removed.
    fn write(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<()>;

    /// Validates that the transaction can be committed.
    ///
    /// On success, returns the commit TID of the transaction.
    fn validate(&mut self) -> Result<Tid>;

    /// Writes the changes made by the transaction to the log entry.
    fn log(&self, log_entry: &mut LogEntry<'_>);

    /// Commits the transaction at concurrency control level.
    ///
    /// "pre" means the commit is not durable yet.
    fn precommit(&mut self, commit_tid: Tid);

    /// Aborts the transaction.
    fn abort(&mut self);
}
