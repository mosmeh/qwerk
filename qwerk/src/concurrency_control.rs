mod optimistic;
mod pessimistic;

pub use optimistic::Optimistic;
pub use pessimistic::Pessimistic;

use crate::Result;
use scc::HashIndex;
use std::ptr::NonNull;

pub trait ConcurrencyControl: Default {
    type Record: 'static;
    type Executor<'a>: TransactionExecutor + 'a
    where
        Self: 'a;

    fn spawn_executor<'a>(
        &'a self,
        index: &'a HashIndex<Box<[u8]>, RecordPtr<Self::Record>>,
    ) -> Self::Executor<'a>;
}

pub trait TransactionExecutor {
    fn begin_transaction(&mut self);
    fn read(&mut self, key: &[u8]) -> Result<Option<&[u8]>>;
    fn write(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<()>;
    fn commit(&mut self) -> Result<()>;
    fn abort(&mut self);
}

#[derive(Copy)]
pub struct RecordPtr<T>(pub(crate) NonNull<T>);

impl<T> Clone for RecordPtr<T> {
    fn clone(&self) -> Self {
        Self(self.0)
    }
}

unsafe impl<T: Sync + Send> Send for RecordPtr<T> {}
unsafe impl<T: Sync + Send> Sync for RecordPtr<T> {}

impl<T> From<NonNull<T>> for RecordPtr<T> {
    fn from(ptr: NonNull<T>) -> Self {
        Self(ptr)
    }
}
