mod optimistic;
mod pessimistic;

pub use optimistic::Optimistic;
pub use pessimistic::Pessimistic;

use crate::{
    epoch::EpochGuard,
    memory_reclamation::Reclaimer,
    persistence::{LogEntry, LogWriter},
    record::Record,
    small_bytes::SmallBytes,
    tid::Tid,
    Index, Result, Shared,
};

pub trait ConcurrencyControl: ConcurrencyControlInternal {}

pub trait ConcurrencyControlInternal: Send + Sync + 'static {
    type Record: Record;
    type Executor<'a>: TransactionExecutor + 'a
    where
        Self: 'a;

    fn init() -> Self;

    /// Loads a log entry into `index`.
    ///
    /// This method is called when the database is opened and before any
    /// transactions are started.
    ///
    /// Implementations should load the log entry
    /// into `index`, and make sure a log entry with the largest `tid` for
    /// `key` ends up in `index`.
    ///
    /// `value` of `None` means a tombstone.
    fn load_log_entry(
        index: &Index<Self::Record>,
        key: SmallBytes,
        value: Option<Box<[u8]>>,
        tid: Tid,
    );

    fn spawn_executor<'a>(
        &'a self,
        index: &'a Index<Self::Record>,
        epoch_guard: EpochGuard<'a>,
        reclaimer: Reclaimer<'a, Self::Record>,
    ) -> Self::Executor<'a>;
}

pub trait TransactionExecutor {
    /// Called before calls to other methods when a transaction begins.
    fn begin_transaction(&mut self) {}

    /// Called after
    /// - a successful call to `precommit`
    /// - a call to `abort`.
    fn end_transaction(&mut self) {}

    /// Reads a key-value pair from the transaction.
    ///
    /// Returns `None` if the key does not exist.
    fn read(&mut self, key: &[u8]) -> Result<Option<&[u8]>>;

    /// Writes a key-value pair into the transaction.
    ///
    /// `value` of `None` means the key is removed.
    fn write(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<()>;

    /// Attempts to commit the transaction.
    ///
    /// Implementations should write modified records to `log_writer`
    /// if the commit succeeds.
    ///
    /// On success, returns a log entry that contains the modifications
    /// made in the transaction.
    fn precommit<'a>(&mut self, log_writer: &'a LogWriter<'a>) -> Result<LogEntry<'a>>;

    /// Aborts the transaction.
    ///
    /// Called when a user requests an abort, or when `Err` is returned from
    /// `read`, `write`, or `precommit`.
    fn abort(&mut self);
}
