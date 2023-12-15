use crate::{
    concurrency_control::TransactionExecutor, ConcurrencyControl, Epoch, Error, Result, Worker,
};

pub struct Transaction<'db, 'worker, C: ConcurrencyControl> {
    worker: &'worker mut Worker<'db, C>,
    is_active: bool,
}

impl<'db, 'worker, C: ConcurrencyControl> Transaction<'db, 'worker, C> {
    pub fn new(worker: &'worker mut Worker<'db, C>) -> Self {
        Self {
            worker,
            is_active: true,
        }
    }

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

    /// Inserts a key-value pair into the database.
    ///
    /// If the key already exists, the value is overwritten.
    pub fn insert<K, V>(&mut self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.try_mutate(|worker| {
            worker
                .txn_executor
                .write(key.as_ref(), Some(value.as_ref()))
        })
    }

    pub fn remove<K: AsRef<[u8]>>(&mut self, key: K) -> Result<()> {
        self.try_mutate(|worker| worker.txn_executor.write(key.as_ref(), None))
    }

    /// Commits the transaction.
    ///
    /// Even if the transaction consists only of [`get`]s, the transaction
    /// must be committed and must succeed.
    /// If the commit fails, [`get`] may have returned inconsistent values,
    /// so the values returned by [`get`] must be discarded.
    ///
    /// # Returns
    /// An epoch at which the transaction was committed.
    ///
    /// [`get`]: #method.get
    pub fn commit(mut self) -> Result<Epoch> {
        let epoch = self.try_mutate(|worker| worker.txn_executor.commit(&mut worker.log_writer))?;
        self.worker.txn_executor.end_transaction();
        self.is_active = false;
        Ok(epoch)
    }

    /// Aborts the transaction.
    ///
    /// All the changes made in the transaction are rolled back.
    pub fn abort(mut self) {
        self.do_abort();
    }

    fn try_mutate<T, F>(&mut self, f: F) -> Result<T>
    where
        F: FnOnce(&mut Worker<C>) -> Result<T>,
    {
        if !self.is_active {
            return Err(Error::AlreadyAborted);
        }
        let result = f(self.worker);
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
