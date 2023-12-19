use crate::{
    concurrency_control::TransactionExecutor, persistence::LogEntry, ConcurrencyControl, Epoch,
    Error, Result, Worker,
};

pub struct Transaction<'db, 'worker, C: ConcurrencyControl> {
    worker: &'worker mut Worker<'db, C>,
    is_active: bool,
}

impl<'db, 'worker, C: ConcurrencyControl> Transaction<'db, 'worker, C> {
    pub(crate) fn new(worker: &'worker mut Worker<'db, C>) -> Self {
        Self {
            worker,
            is_active: true,
        }
    }

    /// Returns the value corresponding to the key.
    ///
    /// Returns `None` if the key does not exist.
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
        self.do_write(key, Some(value))
    }

    /// Removes a key from the database.
    pub fn remove<K: AsRef<[u8]>>(&mut self, key: K) -> Result<()> {
        self.do_write::<_, &[u8]>(key, None)
    }

    fn do_write<K, V>(&mut self, key: K, value: Option<V>) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        if !self.is_active {
            return Err(Error::AlreadyAborted);
        }
        let result = self
            .worker
            .txn_executor
            .write(key.as_ref(), value.as_ref().map(AsRef::as_ref));
        if result.is_err() {
            self.do_abort();
        }
        result
    }

    /// Commits the transaction and waits until the commit is durable.
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
        let persistent_epoch = self.worker.persistent_epoch;
        let log_entry = self.do_precommit()?;
        let commit_epoch = log_entry.epoch();
        log_entry.flush()?; // Even if this fails, we can no longer abort the transaction.
        persistent_epoch.wait_for(commit_epoch);
        Ok(commit_epoch)
    }

    /// Commits the transaction, but does not wait for durability.
    /// This is useful when multiple transactions are committed in a batch.
    ///
    /// To make sure the transaction is durable, wait for the returned
    /// commit epoch to be equal to or greater than [`Database::durable_epoch`].
    ///
    /// For other remarks, see [`commit`].
    ///
    /// [`Database::durable_epoch`]: crate::Database#method.durable_epoch
    /// [`commit`]: #method.commit
    pub fn precommit(mut self) -> Result<Epoch> {
        Ok(self.do_precommit()?.epoch())
    }

    fn do_precommit(&mut self) -> Result<LogEntry> {
        if !self.is_active {
            return Err(Error::AlreadyAborted);
        }
        let log_entry = self
            .worker
            .txn_executor
            .precommit(&self.worker.log_writer)?; // `do_abort` is called in `drop` on `Err`
        self.worker.txn_executor.end_transaction();
        self.is_active = false;
        Ok(log_entry)
    }

    /// Aborts the transaction.
    ///
    /// All the changes made in the transaction are rolled back.
    pub fn abort(mut self) {
        self.do_abort();
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
        self.do_abort();
    }
}
