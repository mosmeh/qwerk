use crate::{
    concurrency_control::TransactionExecutor, ConcurrencyControl, DefaultProtocol, Epoch, Error,
    Result, Worker,
};

/// A transaction.
///
/// All the operations (`get`, `insert`, `remove`, and `commit`) can fail due to
/// conflicts with other concurrent transactions.
/// On failure, the transaction is aborted and
/// [`Error::TransactionNotSerializable`] is returned.
/// You can retry the transaction (possibly after waiting for a while) when this
/// happens.
///
/// All transactions have to be committed or aborted before being dropped.
/// Otherwise the program panics.
///
/// Even if the transaction consists only of `get`s, the transaction
/// still needs to be committed.
/// The failure of the commit indicates that `get`s may have returned
/// inconsistent values (read skew anomaly), so the values returned by `get`s
/// must be discarded.
pub struct Transaction<'db, 'worker, C: ConcurrencyControl = DefaultProtocol> {
    worker: &'worker mut Worker<'db, C>,
    is_active: bool,
    has_writes: bool,
    async_commit: bool,
}

impl<'db, 'worker, C: ConcurrencyControl> Transaction<'db, 'worker, C> {
    pub(crate) fn new(worker: &'worker mut Worker<'db, C>) -> Self {
        Self {
            worker,
            is_active: true,
            has_writes: false,
            async_commit: false,
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
    /// Returns an epoch the transaction belongs to.
    pub fn commit(mut self) -> Result<Epoch> {
        if !self.is_active {
            return Err(Error::TransactionAlreadyAborted);
        }
        let result = self.do_commit();
        if result.is_err() {
            self.do_abort();
        }
        result
    }

    fn do_commit(&mut self) -> Result<Epoch> {
        let txn_executor = &mut self.worker.txn_executor;
        let commit_tid = match &self.worker.persistence {
            Some(persistence) if self.has_writes => {
                let log_writer = persistence.lock_log_writer()?;
                let commit_tid = txn_executor.validate()?;
                let mut log_entry = log_writer.insert_entry(commit_tid);
                txn_executor.log(&mut log_entry);
                txn_executor.precommit(commit_tid);
                txn_executor.end_transaction();
                self.is_active = false;
                if !self.async_commit {
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
        if !self.async_commit {
            if let Some(persistence) = &self.worker.persistence {
                persistence.wait_for_durability(commit_epoch);
            }

            // Ensure recoverability of the transaction by delaying the commit
            // completion until the commit epoch finishes.
            self.worker.epoch_fw.wait_for_reclamation(commit_epoch);
        }

        Ok(commit_epoch)
    }

    /// Aborts the transaction.
    ///
    /// All the changes made in the transaction are rolled back.
    pub fn abort(mut self) {
        self.do_abort();
    }

    fn do_abort(&mut self) {
        if self.is_active {
            self.worker.txn_executor.abort();
            self.worker.txn_executor.end_transaction();
            self.is_active = false;
        }
    }

    /// Sets whether the transaction should be committed asynchronously.
    /// Defaults to `false`.
    ///
    /// If this is set to `true`, [`commit`] does not wait for the transaction
    /// to be committed.
    ///
    /// Setting this to `true` is useful when multiple transactions are
    /// submitted in a batch. To make sure that the transactions are committed,
    /// perform a normal [`commit`] on the last transaction in the batch or
    /// call [`Database::commit_pending`].
    ///
    /// [`commit`]: #method.commit
    /// [`Database::commit_pending`]: crate::Database#method.commit_pending
    pub fn set_async_commit(&mut self, yes: bool) {
        self.async_commit = yes;
    }
}

impl<C: ConcurrencyControl> Drop for Transaction<'_, '_, C> {
    /// Panics if the transaction is not committed or aborted.
    fn drop(&mut self) {
        if !self.is_active {
            return;
        }
        self.do_abort();
        if std::thread::panicking() {
            return;
        }
        panic!("Transaction dropped without being committed or aborted");
    }
}
