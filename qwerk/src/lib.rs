mod bytes_ext;
mod concurrency_control;
mod epoch;
mod lock;
mod persistence;
mod qsbr;
mod shared;
mod signal_channel;
mod slotted_cell;
mod small_bytes;
mod tid;
mod transaction;

pub use concurrency_control::{ConcurrencyControl, Optimistic, Pessimistic};
pub use epoch::Epoch;
pub use transaction::Transaction;

use concurrency_control::TransactionExecutor;
use epoch::EpochFramework;
use persistence::{LogWriter, Logger, LoggerConfig, PersistentEpoch};
use scc::HashIndex;
use shared::Shared;
use small_bytes::SmallBytes;
use std::{num::NonZeroUsize, path::Path, sync::Arc, time::Duration};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Serialization of a transaction failed.
    #[error("serilization of the transaction failed")]
    NotSerializable,

    /// Too many transactions in a single epoch.
    #[error("too many transactions in a single epoch")]
    TooManyTransactions,

    /// Attempted to perform an operation on an aborted transaction.
    #[error("attempted to perform an operation on the aborted transaction")]
    AlreadyAborted,

    /// Database is corrupted or tried to open a non-database path.
    #[error("database is corrupted or tried to open a non-database path")]
    Corrupted,

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct DatabaseOptions {
    background_threads: NonZeroUsize,
    epoch_duration: Duration,
    gc_threshold: usize,
    log_buffer_size_bytes: usize,
    log_buffers_per_worker: NonZeroUsize,
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        Self {
            background_threads: std::thread::available_parallelism()
                .map_or(1, |n| n.get().min(4))
                .try_into()
                .unwrap(),
            epoch_duration: Duration::from_millis(40), // Default in the Silo paper (Tu et al. 2013).
            gc_threshold: 4096,
            log_buffer_size_bytes: 1024 * 1024,
            log_buffers_per_worker: 8.try_into().unwrap(),
        }
    }
}

impl DatabaseOptions {
    pub fn new() -> Self {
        Default::default()
    }

    /// Opens a database at the given path, creating it if it does not exist.
    pub fn open<C, P>(&self, path: P) -> Result<Database<C>>
    where
        C: ConcurrencyControl,
        P: AsRef<Path>,
    {
        let dir = path.as_ref();
        match std::fs::create_dir(dir) {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                if !dir.is_dir() {
                    return Err(Error::Corrupted);
                }
            }
            Err(e) => return Err(e.into()),
        }
        let dir = dir.canonicalize()?;

        let persistent_epoch = PersistentEpoch::new(&dir)?;
        let durable_epoch = persistent_epoch.get();
        let index = persistence::recover::<C>(&dir, durable_epoch, self.background_threads)?;

        let epoch_fw = Arc::new(EpochFramework::new(epoch::Config {
            initial_epoch: durable_epoch.increment(),
            epoch_duration: self.epoch_duration,
        }));

        let persistent_epoch = Arc::new(persistent_epoch);
        let logger = Logger::new(LoggerConfig {
            dir,
            epoch_fw: epoch_fw.clone(),
            persistent_epoch: persistent_epoch.clone(),
            flushing_threads: self.background_threads,
            preallocated_buffer_size: self.log_buffer_size_bytes,
            buffers_per_writer: self.log_buffers_per_worker,
        })?;
        let concurrency_control = C::init(self.gc_threshold);

        Ok(Database {
            index,
            concurrency_control,
            epoch_fw,
            logger,
            persistent_epoch,
        })
    }

    /// The number of background threads used for logging and recovery.
    /// Defaults to the smaller of the number of CPU cores and 4.
    pub fn background_threads(&mut self, n: NonZeroUsize) -> &mut Self {
        self.background_threads = n;
        self
    }

    /// The duration of an epoch. Defaults to 40 milliseconds.
    pub fn epoch_duration(&mut self, duration: Duration) -> &mut Self {
        self.epoch_duration = duration;
        self
    }

    /// Workers perform garbage collection of removed records and old versions
    /// of record values when this number of bytes of garbage is accumulated.
    /// Defaults to 4 KiB.
    pub fn gc_threshold(&mut self, bytes: usize) -> &mut Self {
        self.gc_threshold = bytes;
        self
    }

    /// The size of a log buffer in bytes. Workers pass logs to log flushing
    /// threads in chunks of this size. Defaults to 1 MiB.
    pub fn log_buffer_size(&mut self, bytes: usize) -> &mut Self {
        self.log_buffer_size_bytes = bytes;
        self
    }

    /// The number of log buffers per worker. Defaults to 8.
    pub fn log_buffers_per_worker(&mut self, n: NonZeroUsize) -> &mut Self {
        self.log_buffers_per_worker = n;
        self
    }
}

type Index<T> = HashIndex<SmallBytes, Shared<T>>;

mod record {
    pub trait Record: Send + Sync + 'static {
        fn is_tombstone(&self) -> bool;
    }
}

pub struct Database<C: ConcurrencyControl> {
    index: Index<C::Record>,
    concurrency_control: C,
    epoch_fw: Arc<EpochFramework>,
    logger: Logger,
    persistent_epoch: Arc<PersistentEpoch>,
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
            log_writer: self.logger.spawn_writer(),
            persistent_epoch: &self.persistent_epoch,
        }
    }

    /// Returns the current durable epoch.
    ///
    /// The durable epoch is the epoch up to which all changes made by
    /// committed transactions are guaranteed to be durable.
    pub fn durable_epoch(&self) -> Epoch {
        self.persistent_epoch.get()
    }

    /// Makes sure the changes made by all committed transactions are persisted
    /// to the disk.
    ///
    /// Returns the durable epoch after the flush.
    pub fn flush(&self) -> std::io::Result<Epoch> {
        self.logger.flush()
    }
}

impl<C: ConcurrencyControl> Drop for Database<C> {
    fn drop(&mut self) {
        let guard = scc::ebr::Guard::new();
        for (_, record_ptr) in self.index.iter(&guard) {
            unsafe { record_ptr.drop_in_place() };
        }
    }
}

pub struct Worker<'a, C: ConcurrencyControl + 'a> {
    txn_executor: C::Executor<'a>,
    log_writer: LogWriter<'a>,
    persistent_epoch: &'a PersistentEpoch,
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

        Transaction::new(self)
    }
}
