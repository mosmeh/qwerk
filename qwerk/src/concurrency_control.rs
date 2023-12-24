mod optimistic;
mod pessimistic;

pub use optimistic::Optimistic;
pub use pessimistic::Pessimistic;

use crate::{
    epoch::EpochParticipant,
    memory_reclamation::Reclaimer,
    persistence::{LogEntry, LogWriter},
    record::Record,
    small_bytes::SmallBytes,
    tid::Tid,
    Epoch, Index, Result, Shared,
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

    fn executor<'a>(
        &'a self,
        index: &'a Index<Self::Record>,
        epoch_participant: EpochParticipant<'a>,
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
    /// On success, returns a precommit object.
    fn precommit<'a>(&mut self, log_writer: &'a LogWriter<'a>) -> Result<Precommit<'a>>;

    /// Aborts the transaction.
    ///
    /// Called when a user requests an abort, or when `Err` is returned from
    /// `read`, `write`, or `precommit`.
    fn abort(&mut self);
}

pub struct Precommit<'a> {
    /// The epoch the transaction belongs to.
    epoch: Epoch,

    /// The log entry that contains the modifications made by the transaction.
    /// `None` if the transaction was read-only.
    log_entry: Option<LogEntry<'a>>,
}

impl Precommit<'_> {
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Flushes a log entry to the disk, if any.
    ///
    /// Returns `true` if a log entry was flushed.
    pub fn flush_log_entry(mut self) -> std::io::Result<bool> {
        let Some(log_entry) = self.log_entry.take() else {
            return Ok(false);
        };
        log_entry.flush()?;
        Ok(true)
    }
}
