mod concurrency_control;
mod epoch;
mod lock;
mod qsbr;
mod shared;
mod slotted_cell;

pub use concurrency_control::{ConcurrencyControl, Optimistic, Pessimistic};

use concurrency_control::TransactionExecutor;
use scc::HashIndex;
use shared::Shared;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Serialization failure.
    #[error("serialization failure")]
    NotSerializable,

    /// Attempted to perform an operation on an aborted transaction.
    #[error("attempted to perform an operation on the aborted transaction")]
    AlreadyAborted,
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct Database<C: ConcurrencyControl> {
    index: HashIndex<Box<[u8]>, Shared<C::Record>>,
    concurrency_control: C,
}

impl<C: ConcurrencyControl> Default for Database<C> {
    fn default() -> Self {
        Self {
            index: Default::default(),
            concurrency_control: C::init(),
        }
    }
}

impl<C: ConcurrencyControl> Database<C> {
    pub fn new() -> Self {
        Default::default()
    }
}

impl<C: ConcurrencyControl> Database<C> {
    /// Spawns a [`Worker`], which can be used to perform transactions.
    ///
    /// You usually should spawn one [`Worker`] per thread, and reuse the
    /// [`Worker`] for multiple transactions.
    pub fn spawn_worker(&self) -> Worker<'_, C> {
        Worker {
            txn_executor: self.concurrency_control.spawn_executor(&self.index),
        }
    }
}

impl<C: ConcurrencyControl> Drop for Database<C> {
    fn drop(&mut self) {
        let guard = scc::ebr::Guard::new();
        for (_, record_ptr) in self.index.iter(&guard) {
            // SAFETY: Since we have &mut self, all Executors have already been
            //         dropped, so no one is holding record pointers now.
            let _ = unsafe { Shared::into_box(*record_ptr) };
        }
    }
}

pub struct Worker<'a, C: ConcurrencyControl + 'a> {
    txn_executor: C::Executor<'a>,
}

impl<'db, C: ConcurrencyControl> Worker<'db, C> {
    /// Begins a new transaction.
    ///
    /// A [`Worker`] can only have one active transaction at a time.
    pub fn begin_transaction<'worker>(&'worker mut self) -> Transaction<'db, 'worker, C> {
        // Rather than instantiating a TransactionExecutor every time
        // a transaction begins, the single instance is reused so that buffers
        // allocated by TransactionExecutor can be reused.
        self.txn_executor.begin_transaction();
        Transaction {
            worker: self,
            is_active: true,
        }
    }
}

pub struct Transaction<'db, 'worker, C: ConcurrencyControl> {
    worker: &'worker mut Worker<'db, C>,
    is_active: bool,
}

impl<C: ConcurrencyControl> Transaction<'_, '_, C> {
    pub fn get<K: AsRef<[u8]>>(&mut self, key: K) -> Result<Option<&[u8]>> {
        if !self.is_active {
            return Err(Error::AlreadyAborted);
        }

        let result = self.worker.txn_executor.read(key.as_ref());

        // HACK: workaround for limitation of NLL borrow checker.
        // Returning a reference obtained from self.worker in the Ok branch
        // requires self.worker to be mutably borrowed for the rest of
        // the function, making it impossible to call self.do_abort() in
        // the Err branch.
        // To avoid this, we turn the reference into a pointer and turn it
        // back into a reference, so that the reference is no longer tied
        // to self.worker.
        match result {
            Ok(value) => Ok(value
                .map(|value| unsafe { std::slice::from_raw_parts(value.as_ptr(), value.len()) })),
            // Ok(value) => Ok(value), // This compiles with -Zpolonius
            Err(err) => {
                self.do_abort();
                Err(err)
            }
        }
    }

    pub fn insert<K, V>(&mut self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.try_mutate(|executor| executor.write(key.as_ref(), Some(value.as_ref())))
    }

    pub fn remove<K: AsRef<[u8]>>(&mut self, key: K) -> Result<()> {
        self.try_mutate(|executor| executor.write(key.as_ref(), None))
    }

    /// Commits the transaction.
    ///
    /// Even if the transaction consists only of [`get`]s, the transaction
    /// must be committed and must succeed.
    /// If the commit fails, [`get`] may have returned inconsistent values,
    /// so the values returned by [`get`] must be discarded.
    ///
    /// [`get`]: #method.get
    pub fn commit(mut self) -> Result<()> {
        self.try_mutate(|executor| executor.commit())?;
        self.worker.txn_executor.end_transaction();
        self.is_active = false;
        Ok(())
    }

    /// Aborts the transaction.
    ///
    /// All the changes made in the transaction are rolled back.
    pub fn abort(mut self) {
        self.do_abort();
    }

    fn try_mutate<T, F>(&mut self, f: F) -> Result<T>
    where
        F: FnOnce(&mut C::Executor<'_>) -> Result<T>,
    {
        if !self.is_active {
            return Err(Error::AlreadyAborted);
        }
        let result = f(&mut self.worker.txn_executor);
        if result.is_err() {
            self.do_abort();
        }
        result
    }

    fn do_abort(&mut self) {
        if !self.is_active {
            return;
        }
        self.worker.txn_executor.abort();
        self.worker.txn_executor.end_transaction();
        self.is_active = false;
    }
}

impl<C: ConcurrencyControl> Drop for Transaction<'_, '_, C> {
    /// [`abort`] the transaction if not committed or aborted.
    ///
    /// [`abort`]: #method.abort
    fn drop(&mut self) {
        self.do_abort()
    }
}
