mod optimistic;
mod pessimistic;

pub use optimistic::Optimistic;
pub use pessimistic::Pessimistic;

use crate::{Result, Shared};
use scc::HashIndex;

pub trait ConcurrencyControl: ConcurrencyControlInternal {}

pub trait ConcurrencyControlInternal: Send + Sync + 'static {
    type Record: Send + Sync + 'static;
    type Executor<'a>: TransactionExecutor + 'a
    where
        Self: 'a;

    // We need this because we don't want to expose implementations of
    // `Default` to the user.
    fn init() -> Self;

    fn spawn_executor<'a>(
        &'a self,
        index: &'a HashIndex<Box<[u8]>, Shared<Self::Record>>,
    ) -> Self::Executor<'a>;
}

pub trait TransactionExecutor {
    fn begin_transaction(&mut self);
    fn read(&mut self, key: &[u8]) -> Result<Option<&[u8]>>;
    fn write(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<()>;
    fn commit(&mut self) -> Result<()>;

    /// Aborts the transaction.
    ///
    /// Called when a user requests an abort, or when `Err` is returned from
    /// `read`, `write`, or `commit`.
    fn abort(&mut self);
}
