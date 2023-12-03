mod optimistic;
mod pessimistic;

pub use optimistic::Optimistic;
pub use pessimistic::Pessimistic;

use crate::{
    epoch::{Epoch, EpochGuard},
    log::Logger,
    Index, Result, Shared,
};

const GC_THRESHOLD_BYTES: usize = 4096;

pub trait ConcurrencyControl: ConcurrencyControlInternal {}

pub trait ConcurrencyControlInternal: Send + Sync + 'static {
    type Record: Send + Sync + 'static;
    type Executor<'a>: TransactionExecutor + 'a
    where
        Self: 'a;

    // We need this because we don't want to expose implementations of
    // `Default` to the user.
    fn init() -> Self;

    fn spawn_executor<'a>(&'a self, index: &'a Index<Self::Record>) -> Self::Executor<'a>;
}

pub trait TransactionExecutor {
    fn begin_transaction(&mut self) {}
    fn end_transaction(&mut self) {}

    fn read(&mut self, key: &[u8]) -> Result<Option<&[u8]>>;
    fn write(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<()>;
    fn commit(&mut self, epoch_guard: &EpochGuard, logger: &Logger) -> Result<Epoch>;

    /// Aborts the transaction.
    ///
    /// Called when a user requests an abort, or when `Err` is returned from
    /// `read`, `write`, or `commit`.
    fn abort(&mut self);
}
