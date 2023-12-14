mod concurrency_control;
mod epoch;
mod lock;
mod log;
mod qsbr;
mod recovery;
mod shared;
mod slotted_cell;
mod small_bytes;
mod tid;

pub use concurrency_control::{ConcurrencyControl, Optimistic, Pessimistic};
pub use epoch::Epoch;

use concurrency_control::TransactionExecutor;
use epoch::EpochFramework;
use log::{LogSystem, Logger};
use scc::HashIndex;
use shared::Shared;
use small_bytes::SmallBytes;
use std::{path::Path, time::Duration};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Serialization failure.
    #[error("serialization failure")]
    NotSerializable,

    /// Too many transactions in a single epoch.
    #[error("too many transactions in a single epoch")]
    TooManyTransactions,

    /// Attempted to perform an operation on an aborted transaction.
    #[error("attempted to perform an operation on the aborted transaction")]
    AlreadyAborted,

    /// Database is corrupted or tried to open a non-database directory.
    #[error("database is corrupted or tried to open a non-database directory")]
    Corrupted,

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct DatabaseOptions {
    background_threads: usize,
    epoch_duration: Duration,
    gc_threshold: usize,
    log_buffer_size_bytes: usize,
    log_buffers_per_worker: usize,
    fsync: bool,
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        Self {
            background_threads: match std::thread::available_parallelism() {
                Ok(n) => n.get().min(4),
                Err(_) => 1,
            },
            epoch_duration: Duration::from_millis(40), // Default in the Silo paper (Tu et al. 2013).
            gc_threshold: 4096,
            log_buffer_size_bytes: 1024 * 1024,
            log_buffers_per_worker: 8,
            fsync: true,
        }
    }
}

impl DatabaseOptions {
    pub fn new() -> Self {
        Default::default()
    }

    /// Opens a database at the given path, creating it if it does not exist.
    pub fn open<C, P>(self, path: P) -> Result<Database<C>>
    where
        C: ConcurrencyControl,
        P: AsRef<Path>,
    {
        let dir = path.as_ref();
        if dir.exists() && !dir.is_dir() {
            return Err(Error::Corrupted);
        }

        let epoch_fw = EpochFramework::new(epoch::Config {
            initial_epoch: Epoch::ZERO,
            epoch_duration: self.epoch_duration,
        });
        let log_system = LogSystem::new(log::Config {
            dir: dir.to_path_buf(),
            flushing_threads: self.background_threads,
            preallocated_buffer_size: self.log_buffer_size_bytes,
            buffers_per_logger: self.log_buffers_per_worker,
            fsync: self.fsync,
        })?;
        let concurrency_control = C::init(self.gc_threshold);

        Ok(Database {
            index: Default::default(),
            concurrency_control,
            epoch_fw,
            log_system,
        })
    }

    /// The number of background threads used for logging.
    /// Defaults to the smaller of the number of CPU cores and 4.
    pub const fn background_threads(mut self, n: usize) -> Self {
        self.background_threads = n;
        self
    }

    /// The duration of an epoch. Defaults to 40 milliseconds.
    pub const fn epoch_duration(mut self, duration: Duration) -> Self {
        self.epoch_duration = duration;
        self
    }

    /// Workers perform garbage collection of removed records when this number
    /// of bytes of garbage is accumulated. Defaults to 4 KiB.
    pub const fn gc_threshold(mut self, bytes: usize) -> Self {
        self.gc_threshold = bytes;
        self
    }

    /// The size of a log buffer in bytes. Workers pass logs to log flushing
    /// threads in chunks of this size. Defaults to 1 MiB.
    pub const fn log_buffer_size(mut self, bytes: usize) -> Self {
        self.log_buffer_size_bytes = bytes;
        self
    }

    /// The number of log buffers per worker. Defaults to 8.
    pub const fn log_buffers_per_worker(mut self, n: usize) -> Self {
        self.log_buffers_per_worker = n;
        self
    }

    /// Whether to call `fsync` after writing to a disk. Defaults to `true`.
    pub const fn fsync(mut self, yes: bool) -> Self {
        self.fsync = yes;
        self
    }
}

type Index<T> = HashIndex<SmallBytes, Shared<T>>;

pub struct Database<C: ConcurrencyControl> {
    index: Index<C::Record>,
    concurrency_control: C,
    epoch_fw: EpochFramework,
    log_system: LogSystem,
}

impl<C: ConcurrencyControl> Database<C> {
    /// Opens a database at the given path, creating it if it does not exist.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        DatabaseOptions::new().open(path)
    }

    /// Spawns a [`Worker`], which can be used to perform transactions.
    ///
    /// You usually should spawn one [`Worker`] per thread, and reuse the
    /// [`Worker`] for multiple transactions.
    pub fn spawn_worker(&self) -> Worker<C> {
        let epoch_guard = self.epoch_fw.acquire();
        Worker {
            txn_executor: self
                .concurrency_control
                .spawn_executor(&self.index, epoch_guard),
            logger: self.log_system.spawn_logger(),
        }
    }

    /// Returns the current durable epoch.
    ///
    /// The durable epoch is the epoch up to which all changes made by
    /// committed transactions are guaranteed to be durable.
    pub fn durable_epoch(&self) -> Epoch {
        self.log_system.durable_epoch()
    }

    /// Makes sure the changes made by all committed transactions are persisted
    /// to the disk.
    pub fn flush(&self) -> std::io::Result<()> {
        self.log_system.flush()
    }
}

impl<C: ConcurrencyControl> Drop for Database<C> {
    fn drop(&mut self) {
        let guard = scc::ebr::Guard::new();
        for (_, record_ptr) in self.index.iter(&guard) {
            // SAFETY: Since we have &mut self, all Executors have already been
            //         dropped, so no one is holding record pointers now.
            unsafe { record_ptr.drop_in_place() };
        }
    }
}

pub struct Worker<'a, C: ConcurrencyControl + 'a> {
    txn_executor: C::Executor<'a>,
    logger: Logger<'a>,
}

static_assertions::assert_not_impl_any!(Worker<'_, Pessimistic>: Send, Sync);
static_assertions::assert_not_impl_any!(Worker<'_, Optimistic>: Send, Sync);

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
        let epoch = self.try_mutate(|worker| worker.txn_executor.commit(&worker.logger))?;
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
