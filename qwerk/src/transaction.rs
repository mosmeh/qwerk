use crate::{
    concurrency_control::TransactionExecutor, ConcurrencyControl, DefaultProtocol, Epoch, Error,
    Result, Worker,
};

pub struct Transaction<'db, 'worker, C: ConcurrencyControl = DefaultProtocol> {
    worker: &'worker mut Worker<'db, C>,
    is_active: bool,
    has_writes: bool,
    wait_for_durability: bool,
}

impl<'db, 'worker, C: ConcurrencyControl> Transaction<'db, 'worker, C> {
    pub(crate) fn new(worker: &'worker mut Worker<'db, C>) -> Self {
        Self {
            worker,
            is_active: true,
            has_writes: false,
            wait_for_durability: true,
        }
    }

    /// Returns the value corresponding to the key.
    ///
    /// Returns `None` if the key does not exist.
    pub fn get<K: AsRef<[u8]>>(&mut self, key: K) -> Result<Option<&[u8]>> {
        if !self.is_active {
            return Err(Error::TransactionAlreadyAborted);
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
            return Err(Error::TransactionAlreadyAborted);
        }
        let result = self
            .worker
            .txn_executor
            .write(key.as_ref(), value.as_ref().map(AsRef::as_ref));
        match result {
            Ok(()) => self.has_writes = true,
            Err(_) => self.do_abort(),
        }
        result
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
        if !self.is_active {
            return Err(Error::TransactionAlreadyAborted);
        }

        // This method takes an ownership of self, so if it returns an error
        // while is_active is true, self is dropped and the transaction is
        // aborted.

        let txn_executor = &mut self.worker.txn_executor;
        let commit_tid = match &self.worker.persistence {
            Some(persistence) if self.has_writes => {
                let log_writer = persistence.log_writer();
                let commit_tid = txn_executor.validate()?;
                let mut log_entry = log_writer.insert_entry(commit_tid);
                txn_executor.log(&mut log_entry);
                txn_executor.precommit(commit_tid);
                txn_executor.end_transaction();
                self.is_active = false;
                if self.wait_for_durability {
                    log_entry.flush()?;
                }
                commit_tid
            }
            _ => {
                let commit_tid = txn_executor.validate()?;
                txn_executor.precommit(commit_tid);
                txn_executor.end_transaction();
                self.is_active = false;
                commit_tid
            }
        };

        let commit_epoch = commit_tid.epoch();
        match &self.worker.persistence {
            Some(persistence) if self.wait_for_durability => {
                persistence.wait_for_durability(commit_epoch);
            }
            _ => {}
        }
        Ok(commit_epoch)
    }

    /// Aborts the transaction.
    ///
    /// All the changes made in the transaction are rolled back.
    pub fn abort(self) {
        // Drop self.
    }

    fn do_abort(&mut self) {
        assert!(self.is_active);
        self.worker.txn_executor.abort();
        self.worker.txn_executor.end_transaction();
        self.is_active = false;
    }

    /// Sets whether to wait until the transaction becomes durable when
    /// committing the transaction.
    /// Defaults to `true`.
    ///
    /// Setting this to `false` is useful when multiple transactions are
    /// committed in a batch. To make sure that the transactions are durable in
    /// this case, perform a normal commit on the last transaction in the batch
    /// or call [`Database::flush`].
    ///
    /// [`Database::flush`]: crate::Database#method.flush
    pub fn wait_for_durability(&mut self, yes: bool) {
        self.wait_for_durability = yes;
    }
}

impl<C: ConcurrencyControl> Drop for Transaction<'_, '_, C> {
    /// [`abort`] the transaction if not committed or aborted.
    ///
    /// [`abort`]: #method.abort
    fn drop(&mut self) {
        if self.is_active {
            self.do_abort();
        }
    }
}
