mod optimistic;
mod pessimistic;

pub use optimistic::Optimistic;
pub use pessimistic::Pessimistic;

use crate::{
    epoch::{Epoch, EpochGuard},
    persistence::LogWriter,
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

    fn init(gc_threshold: usize) -> Self;

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
    ) -> Self::Executor<'a>;
}

pub trait TransactionExecutor {
    fn begin_transaction(&mut self) {}
    fn end_transaction(&mut self) {}

    fn read(&mut self, key: &[u8]) -> Result<Option<&[u8]>>;
    fn write(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<()>;
    fn commit(&mut self, log_writer: &LogWriter) -> Result<Epoch>;

    /// Aborts the transaction.
    ///
    /// Called when a user requests an abort, or when `Err` is returned from
    /// `read`, `write`, or `commit`.
    fn abort(&mut self);
}
