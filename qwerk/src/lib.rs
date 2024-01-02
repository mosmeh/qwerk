mod bytes_ext;
mod concurrency_control;
mod epoch;
mod file_lock;
mod lock;
mod memory_reclamation;
mod persistence;
mod shared;
mod signal_channel;
mod slotted_cell;
mod small_bytes;
mod tid;
mod transaction;

pub use concurrency_control::{ConcurrencyControl, DefaultProtocol, Optimistic, Pessimistic};
pub use epoch::Epoch;
pub use transaction::Transaction;

use concurrency_control::TransactionExecutor;
use epoch::EpochFramework;
use file_lock::FileLock;
use memory_reclamation::MemoryReclamation;
use persistence::{
    CheckpointerConfig, IoMonitor, LoggerConfig, Persistence, PersistenceHandle, PersistentEpoch,
};
use scc::HashIndex;
use shared::Shared;
use small_bytes::SmallBytes;
use std::{num::NonZeroUsize, path::Path, sync::Arc, time::Duration};

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Database is already open.
    #[error("Database is already open")]
    DatabaseAlreadyOpen,

    /// Database is corrupted or tried to open a non-database path.
    #[error("Database is corrupted or tried to open a non-database path")]
    DatabaseCorrupted,

    /// Serialization of a transaction failed.
    #[error("Serilization of the transaction failed")]
    TransactionNotSerializable,

    /// Too many transactions in a single epoch.
    #[error("Too many transactions in a single epoch")]
    TooManyTransactions,

    /// Attempted to perform an operation on an aborted transaction.
    #[error("Attempted to perform an operation on the aborted transaction")]
    TransactionAlreadyAborted,

    /// Persistence failed due to I/O errors.
    #[error("Persistence failed due to I/O errors. Reopen the database after fixing the errors.")]
    PersistenceFailed,

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct DatabaseOptions<C: ConcurrencyControl = DefaultProtocol> {
    concurrency_control: C,
    epoch_duration: Duration,
    gc_threshold: usize,
    recovery_threads: NonZeroUsize,
    logging_threads: NonZeroUsize,
    checkpoint_interval: Duration,
    log_buffer_size: usize,
    log_buffers_per_worker: NonZeroUsize,
    max_file_size: usize,
}

impl<C: ConcurrencyControl> Default for DatabaseOptions<C> {
    fn default() -> Self {
        let num_cpus =
            std::thread::available_parallelism().unwrap_or_else(|_| 1.try_into().unwrap());
        Self {
            concurrency_control: Default::default(),
            epoch_duration: Duration::from_millis(40), // Default in the Silo paper (Tu et al. 2013).
            gc_threshold: 4096,
            recovery_threads: num_cpus,
            logging_threads: 1.try_into().unwrap(),
            checkpoint_interval: Duration::from_secs(10),
            log_buffer_size: 1024 * 1024,
            log_buffers_per_worker: 8.try_into().unwrap(),
            max_file_size: 32 * 1024 * 1024,
        }
    }
}

impl DatabaseOptions<DefaultProtocol> {
    pub fn new() -> Self {
        Default::default()
    }
}

impl<C: ConcurrencyControl> DatabaseOptions<C> {
    /// Creates a new options object with the given concurrency control
    /// protocol.
    pub fn with_concurrency_control(concurrency_control: C) -> Self {
        Self {
            concurrency_control,
            ..Default::default()
        }
    }

    /// Opens a database at the given path, creating it if it does not exist.
    pub fn open<P: AsRef<Path>>(self, path: P) -> Result<Database<C>> {
        let dir = path.as_ref();
        match std::fs::create_dir(dir) {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                if !dir.is_dir() {
                    return Err(Error::DatabaseCorrupted);
                }
            }
            Err(e) => return Err(e.into()),
        }
        let dir = dir.canonicalize()?;
        let lock =
            FileLock::try_lock_exclusive(dir.join("lock"))?.ok_or(Error::DatabaseAlreadyOpen)?;

        let persistent_epoch = Arc::new(PersistentEpoch::new(&dir)?);
        let (index, initial_epoch) = persistence::recover::<C>(
            &dir,
            &persistent_epoch,
            self.recovery_threads,
            self.max_file_size,
        )?;
        let epoch_fw = Arc::new(EpochFramework::new(initial_epoch, self.epoch_duration));
        let index = Arc::new(index);
        let reclamation = Arc::new(MemoryReclamation::new(self.gc_threshold));
        let io_monitor = Arc::new(IoMonitor::default());

        let persistence = Persistence::new(
            lock,
            persistent_epoch.clone(),
            LoggerConfig {
                dir: dir.clone(),
                epoch_fw: epoch_fw.clone(),
                persistent_epoch: persistent_epoch.clone(),
                io_monitor: io_monitor.clone(),
                flushing_threads: self.logging_threads,
                preallocated_buffer_size: self.log_buffer_size,
                buffers_per_writer: self.log_buffers_per_worker,
                max_file_size: self.max_file_size,
            },
            CheckpointerConfig {
                dir,
                index: index.clone(),
                epoch_fw: epoch_fw.clone(),
                reclamation: reclamation.clone(),
                persistent_epoch,
                io_monitor,
                interval: self.checkpoint_interval,
                max_file_size: self.max_file_size,
            },
        )?;

        Ok(Database {
            index,
            concurrency_control: self.concurrency_control,
            epoch_fw,
            reclamation,
            persistence: Some(persistence),
        })
    }

    /// Opens a temporary database that is not persisted to the disk.
    pub fn open_temporary(self) -> Database<C> {
        Database {
            index: Default::default(),
            concurrency_control: self.concurrency_control,
            epoch_fw: EpochFramework::new(Epoch(0), self.epoch_duration).into(),
            reclamation: MemoryReclamation::new(self.gc_threshold).into(),
            persistence: None,
        }
    }

    /// The duration of an epoch. Defaults to 40 milliseconds.
    #[must_use]
    pub fn epoch_duration(mut self, duration: Duration) -> Self {
        self.epoch_duration = duration;
        self
    }

    /// Workers perform garbage collection of removed records and old versions
    /// of record values when this number of bytes of garbage is accumulated.
    /// Defaults to 4 KiB.
    #[must_use]
    pub fn gc_threshold(mut self, bytes: usize) -> Self {
        self.gc_threshold = bytes;
        self
    }

    /// The number of threads used for recovery.
    /// Defaults to the number of CPU cores.
    #[must_use]
    pub fn recovery_threads(mut self, n: NonZeroUsize) -> Self {
        self.recovery_threads = n;
        self
    }

    /// The number of background threads used for logging.
    /// Defaults to 1.
    #[must_use]
    pub fn logging_threads(mut self, n: NonZeroUsize) -> Self {
        self.logging_threads = n;
        self
    }

    /// The interval between checkpoints. Defaults to 10 seconds.
    #[must_use]
    pub fn checkpoint_interval(mut self, interval: Duration) -> Self {
        self.checkpoint_interval = interval;
        self
    }

    /// The size of a log buffer in bytes. Workers pass logs to log flushing
    /// threads in chunks of this size. Defaults to 1 MiB.
    #[must_use]
    pub fn log_buffer_size(mut self, bytes: usize) -> Self {
        self.log_buffer_size = bytes;
        self
    }

    /// The number of log buffers per worker. Defaults to 8.
    #[must_use]
    pub fn log_buffers_per_worker(mut self, n: NonZeroUsize) -> Self {
        self.log_buffers_per_worker = n;
        self
    }

    /// The maximum size of a single chunk of a checkpoint or log file.
    /// Defaults to 32 MiB.
    #[must_use]
    pub fn max_file_size(mut self, bytes: usize) -> Self {
        self.max_file_size = bytes;
        self
    }
}

type Index<T> = HashIndex<SmallBytes, Shared<T>>;

mod record {
    use crate::tid::Tid;

    pub trait Record: Send + Sync + 'static {
        /// Peeks at the value of the record if the record is not a tombstone.
        ///
        /// Returns the result of `f` was called.
        fn peek<F, T>(&self, f: F) -> Option<T>
        where
            F: FnOnce(&[u8], Tid) -> T;

        /// Returns `true` if the record is a tombstone.
        ///
        /// A tombstone is a record that is considered to be deleted.
        fn is_tombstone(&self) -> bool;
    }
}

pub struct Database<C: ConcurrencyControl = DefaultProtocol> {
    index: Arc<Index<C::Record>>,
    concurrency_control: C,
    epoch_fw: Arc<EpochFramework>,
    reclamation: Arc<MemoryReclamation>,
    persistence: Option<Persistence>,
}

impl Database<DefaultProtocol> {
    /// Opens a database at the given path, creating it if it does not exist.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        DatabaseOptions::new().open(path)
    }

    /// Opens a temporary database that is not persisted to the disk.
    pub fn open_temporary() -> Self {
        DatabaseOptions::new().open_temporary()
    }
}

impl<C: ConcurrencyControl> Database<C> {
    /// Spawns a [`Worker`], which can be used to perform transactions.
    ///
    /// You usually should spawn one [`Worker`] per thread, and reuse the
    /// [`Worker`] for multiple transactions.
    pub fn worker(&self) -> Result<Worker<C>> {
        let epoch_participant = self.epoch_fw.participant();
        let reclaimer = self.reclamation.reclaimer();
        Ok(Worker {
            txn_executor: self.concurrency_control.executor(
                &self.index,
                epoch_participant,
                reclaimer,
            ),
            persistence: self
                .persistence
                .as_ref()
                .map(Persistence::handle)
                .transpose()?,
        })
    }

    /// Returns the current durable epoch.
    ///
    /// The durable epoch is the epoch up to which all changes made by
    /// committed transactions are guaranteed to be durable.
    ///
    /// Always returns `Epoch(0)` if the database is temporary.
    pub fn durable_epoch(&self) -> Epoch {
        self.persistence
            .as_ref()
            .map_or(Epoch(0), Persistence::durable_epoch)
    }

    /// Makes sure the changes made by all committed transactions are persisted
    /// to the disk.
    ///
    /// This operation is a no-op if the database is temporary.
    ///
    /// Returns the durable epoch after the flush.
    pub fn flush(&self) -> Result<Epoch> {
        Ok(match &self.persistence {
            Some(p) => p.flush()?,
            None => Epoch(0),
        })
    }
}

impl<C: ConcurrencyControl> Drop for Database<C> {
    fn drop(&mut self) {
        self.persistence.take();

        let guard = scc::ebr::Guard::new();
        for (_, record_ptr) in self.index.iter(&guard) {
            unsafe { record_ptr.drop_in_place() };
        }
    }
}

pub struct Worker<'a, C: ConcurrencyControl + 'a = DefaultProtocol> {
    txn_executor: C::Executor<'a>,
    persistence: Option<PersistenceHandle<'a>>,
}

static_assertions::assert_not_impl_any!(Worker<'_, Pessimistic>: Send, Sync);
static_assertions::assert_not_impl_any!(Worker<'_, Optimistic>: Send, Sync);

impl<'db, C: ConcurrencyControl> Worker<'db, C> {
    /// Begins a new transaction.
    ///
    /// A [`Worker`] can only have one active transaction at a time.
    pub fn transaction<'worker>(&'worker mut self) -> Transaction<'db, 'worker, C> {
        // Rather than instantiating a TransactionExecutor every time
        // a transaction begins, the single instance is reused so that buffers
        // allocated by TransactionExecutor can be reused.
        self.txn_executor.begin_transaction();

        Transaction::new(self)
    }
}
