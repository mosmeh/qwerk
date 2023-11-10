pub mod concurrency_control;

mod epoch;
mod lock;
mod qsbr;
mod slotted_cell;

use concurrency_control::{ConcurrencyControl, RecordPtr, TransactionExecutor};
use scc::HashIndex;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("serialization failure")]
    NotSerializable,

    #[error("attempted to perform an operation on the aborted transaction")]
    AlreadyAborted,
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct Database<C: ConcurrencyControl> {
    index: HashIndex<Box<[u8]>, RecordPtr<C::Record>>,
    concurrency_control: C,
}

impl<C: ConcurrencyControl> Default for Database<C> {
    fn default() -> Self {
        Self {
            index: Default::default(),
            concurrency_control: Default::default(),
        }
    }
}

impl<C: ConcurrencyControl> Database<C> {
    pub fn new() -> Self {
        Default::default()
    }
}

impl<C: ConcurrencyControl> Database<C> {
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
            let _ = unsafe { Box::from_raw(record_ptr.0.as_ptr()) };
        }
    }
}

pub struct Worker<'a, C: ConcurrencyControl + 'a> {
    txn_executor: C::Executor<'a>,
}

impl<'db, C: ConcurrencyControl> Worker<'db, C> {
    pub fn begin_transaction<'worker>(&'worker mut self) -> Transaction<'db, 'worker, C> {
        // Rather than instantiating a TransactionExecutor every time
        // a transaction begins, the single instance is reused so that allocated
        // buffers can be reused.
        self.txn_executor.begin_transaction();
        Transaction {
            worker: self,
            is_running: true,
        }
    }
}

pub struct Transaction<'db, 'worker, C: ConcurrencyControl> {
    worker: &'worker mut Worker<'db, C>,
    is_running: bool,
}

impl<C: ConcurrencyControl> Transaction<'_, '_, C> {
    pub fn get<K: AsRef<[u8]>>(&mut self, key: K) -> Result<Option<Vec<u8>>> {
        self.do_operation(|executor| executor.read(key.as_ref()))
    }

    pub fn insert<K, V>(&mut self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.do_operation(|executor| executor.write(key.as_ref(), Some(value.as_ref())))
    }

    pub fn remove<K: AsRef<[u8]>>(&mut self, key: K) -> Result<()> {
        self.do_operation(|executor| executor.write(key.as_ref(), None))
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
        self.do_operation(|executor| executor.commit())?;
        self.is_running = false;
        Ok(())
    }

    /// Aborts the transaction.
    ///
    /// All the changes made in the transaction are rolled back.
    pub fn abort(mut self) {
        self.do_abort();
    }

    fn do_operation<T, F>(&mut self, f: F) -> Result<T>
    where
        F: FnOnce(&mut C::Executor<'_>) -> Result<T>,
    {
        if !self.is_running {
            return Err(Error::AlreadyAborted);
        }
        match f(&mut self.worker.txn_executor) {
            Ok(value) => Ok(value),
            Err(err) => {
                self.do_abort();
                Err(err)
            }
        }
    }

    fn do_abort(&mut self) {
        if !self.is_running {
            return;
        }
        self.is_running = false;
        self.worker.txn_executor.abort();
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
