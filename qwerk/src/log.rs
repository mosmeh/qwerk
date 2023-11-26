use crate::{
    epoch::Epoch,
    slotted_cell::{Slot, SlottedCell},
};
use crossbeam_channel::{Receiver, Sender};
use crossbeam_queue::ArrayQueue;
use parking_lot::{Mutex, MutexGuard};
use std::{
    cell::OnceCell,
    fs::{DirBuilder, File},
    io::{IoSlice, Write},
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering::SeqCst},
        Arc,
    },
    thread::JoinHandle,
    time::Duration,
};

pub const DATA_DIR: &str = "data";
pub const PEPOCH_FILENAME: &str = "data/pepoch";
const TMP_PEPOCH_FILENAME: &str = "data/pepoch.tmp";

const NUM_WRITERS: usize = 4;
const PREALLOCATED_BUF_SIZE: usize = 1024 * 1024;
const NUM_BUFS_PER_LOGGER: usize = 4;

/// A redo logging system.
pub struct LogSystem {
    loggers: Arc<SlottedCell<LoggerInner>>,
    durable_epoch: Arc<DurableEpoch>,
    flush_req_tx: Option<Sender<FlushRequest>>,
    writers: Vec<JoinHandle<()>>,
    daemon: Option<JoinHandle<()>>,
    is_running: Arc<AtomicBool>,
}

impl LogSystem {
    pub fn new() -> std::io::Result<Self> {
        DirBuilder::new().recursive(true).create(DATA_DIR)?;

        let loggers = Arc::new(SlottedCell::<LoggerInner>::default());

        // flush_req is not a member of LoggerInner, because we want to tell
        // writers to exit by closing the channel, but the writers hold
        // references to LoggerInner.
        let (flush_req_tx, flush_req_rx) = crossbeam_channel::unbounded::<FlushRequest>();

        let writers = (0..NUM_WRITERS)
            .map(|_| {
                let loggers = loggers.clone();
                let flush_req_rx = flush_req_rx.clone();
                std::thread::spawn(move || {
                    while let Ok(req) = flush_req_rx.recv() {
                        loggers.get(req.logger_id).unwrap().flush().unwrap();
                    }
                })
            })
            .collect();

        let durable_epoch = Arc::new(DurableEpoch::new(loggers.clone()));
        let is_running = Arc::new(AtomicBool::new(true));
        let daemon = {
            let loggers = loggers.clone();
            let durable_epoch = durable_epoch.clone();
            let flush_req_tx = flush_req_tx.clone();
            let is_running = is_running.clone();
            std::thread::Builder::new()
                .name("log_system_daemon".into())
                .spawn(move || run_daemon(&loggers, &durable_epoch, flush_req_tx, &is_running))
                .unwrap()
        };

        Ok(Self {
            loggers,
            durable_epoch,
            flush_req_tx: Some(flush_req_tx),
            writers,
            daemon: Some(daemon),
            is_running,
        })
    }

    pub fn spawn_logger(&self) -> Logger {
        Logger {
            inner: self.loggers.alloc_with(LoggerInner::new),
            flush_req_tx: self.flush_req_tx.clone().unwrap(),
        }
    }

    pub fn durable_epoch(&self) -> Epoch {
        self.durable_epoch.load()
    }

    pub fn flush(&self) -> std::io::Result<()> {
        for logger in self.loggers.iter() {
            let mut current_buf = logger.current_buf.lock();
            if current_buf
                .as_ref()
                .map(|buf| !buf.bytes.is_empty())
                .unwrap_or_default()
            {
                logger
                    .flush_queue
                    .push(current_buf.take().unwrap())
                    .unwrap();
            }
            logger.flush()?;
            assert!(current_buf.is_none());
        }
        self.durable_epoch.update()?;
        Ok(())
    }
}

impl Drop for LogSystem {
    fn drop(&mut self) {
        self.is_running.store(false, SeqCst);
        self.flush_req_tx.take().unwrap();
        self.daemon.take().unwrap().join().unwrap();
        for writer in self.writers.drain(..) {
            writer.join().unwrap();
        }
        let _ = self.flush();
    }
}

// The daemon serves two purposes:
// 1. It periodically queues current_buf to flush_queue.
//    This is needed because the TransactionExecutors may not process further
//    transactions, and in such case the current_buf will never be queued.
//    The daemon ensures that the current_buf is queued at some point.
// 2. It periodically updates the durable epoch by calculating the minimum
//    durable epoch of all loggers.
fn run_daemon(
    loggers: &SlottedCell<LoggerInner>,
    durable_epoch: &DurableEpoch,
    flush_req_tx: Sender<FlushRequest>,
    is_running: &AtomicBool,
) {
    while is_running.load(SeqCst) {
        for logger in loggers.iter() {
            // Failure of this lock means that the logger is writing to
            // the buffer. In that case we can just let the logger
            // queue the buffer by itself if needed.
            let Some(mut current_buf) = logger.current_buf.try_lock() else {
                continue;
            };
            if current_buf
                .as_ref()
                .map(|buf| !buf.bytes.is_empty())
                .unwrap_or_default()
            {
                logger.queue(current_buf.take().unwrap(), &flush_req_tx);
            }
        }
        durable_epoch.update().unwrap();
        std::thread::sleep(Duration::from_millis(40));
    }
}

struct DurableEpoch {
    epoch: AtomicU32,
    mutex: Mutex<()>,
    loggers: Arc<SlottedCell<LoggerInner>>,
}

impl DurableEpoch {
    fn new(loggers: Arc<SlottedCell<LoggerInner>>) -> Self {
        Self {
            epoch: Default::default(),
            mutex: Default::default(),
            loggers,
        }
    }

    fn load(&self) -> Epoch {
        Epoch(self.epoch.load(SeqCst))
    }

    fn update(&self) -> std::io::Result<()> {
        let _guard = self.mutex.lock();

        let new_epoch = self
            .loggers
            .iter()
            .map(|logger| Epoch(logger.durable_epoch.load(SeqCst)))
            .min();
        let Some(new_epoch) = new_epoch else {
            return Ok(());
        };

        {
            let mut tmp_pepoch = File::create(TMP_PEPOCH_FILENAME)?;
            tmp_pepoch.write_all(&new_epoch.0.to_le_bytes())?;
            tmp_pepoch.sync_data()?;
        }

        // Atomically replace the file.
        std::fs::rename(TMP_PEPOCH_FILENAME, PEPOCH_FILENAME)?;

        let prev_epoch = Epoch(self.epoch.swap(new_epoch.0, SeqCst));
        assert!(prev_epoch <= new_epoch);

        Ok(())
    }
}

struct FlushRequest {
    logger_id: usize,
}

// Architecture (single logger):
//                                            +---- flush_req ----+
//                                            |                   |
//                                            |                   v
//                      write               queue               flush
// TransactionExecutor ------> current_buf ------> flush_queue ------> file
//                                  ^                    |        |
//                                  |                    |        v
//                                  +---- free_bufs <----+   durable_epoch

pub struct Logger<'a> {
    inner: Slot<'a, LoggerInner>,
    flush_req_tx: Sender<FlushRequest>,
}

impl Logger<'_> {
    pub const fn reserver(&self) -> Reserver {
        Reserver {
            logger: self,
            num_bytes: std::mem::size_of::<u64>() * 2, // tid, num_records
        }
    }

    fn queue(&self, buf: LogBuf) {
        self.inner.queue(buf, &self.flush_req_tx);
    }
}

pub struct Reserver<'a> {
    logger: &'a Logger<'a>,
    num_bytes: usize,
}

impl<'a> Reserver<'a> {
    pub fn reserve_write(&mut self, key: &[u8], value: Option<&[u8]>) {
        self.num_bytes += key.len() + std::mem::size_of::<u64>() * 2; // key, key.len(), and value.len()
        if let Some(value) = value {
            self.num_bytes += value.len();
        }
    }

    pub fn finish(self) -> ReservedCapacity<'a> {
        let mut buf = self.logger.inner.current_buf.lock();
        if buf
            .as_ref()
            .map(|buf| {
                !buf.bytes.is_empty() && buf.bytes.len() + self.num_bytes > PREALLOCATED_BUF_SIZE
            })
            .unwrap_or_default()
        {
            self.logger.queue(buf.take().unwrap());
        }
        if buf.is_none() {
            *buf = Some(LogBuf::new(self.logger.inner.free_bufs_rx.recv().unwrap()));
        }
        buf.as_mut().unwrap().bytes.reserve(self.num_bytes);
        ReservedCapacity {
            logger: self.logger,
            buf,
        }
    }
}

pub struct ReservedCapacity<'a> {
    logger: &'a Logger<'a>,
    buf: MutexGuard<'a, Option<LogBuf>>,
}

impl<'a> ReservedCapacity<'a> {
    pub fn insert(mut self, epoch: Epoch, tid: u64) -> Entry<'a> {
        let buf = self.buf.as_mut().unwrap();
        buf.bytes.extend_from_slice(&tid.to_le_bytes());

        let num_records_offset = buf.bytes.len();
        buf.bytes.extend_from_slice(&u64::MAX.to_le_bytes()); // placeholder

        // Epoch must be monotonically increasing.
        let min_epoch = *buf.min_epoch.get_or_init(|| epoch);
        assert!(min_epoch <= epoch);

        Entry {
            logger: self.logger,
            buf: self.buf,
            num_records_offset,
            num_records: 0,
        }
    }
}

pub struct Entry<'a> {
    logger: &'a Logger<'a>,
    buf: MutexGuard<'a, Option<LogBuf>>,
    num_records_offset: usize,
    num_records: usize,
}

impl Entry<'_> {
    pub fn write(&mut self, key: &[u8], value: Option<&[u8]>) {
        let bytes = &mut self.buf.as_mut().unwrap().bytes;
        bytes.extend_from_slice(&(key.len() as u64).to_le_bytes());
        bytes.extend_from_slice(key);
        match value {
            Some(value) => {
                bytes.extend_from_slice(&(value.len() as u64).to_le_bytes());
                bytes.extend_from_slice(value);
            }
            None => {
                // We can never hold u64::MAX bytes in memory.
                // So we use u64::MAX to represent None.
                bytes.extend_from_slice(&u64::MAX.to_le_bytes());
            }
        }
        self.num_records += 1;
    }
}

impl Drop for Entry<'_> {
    fn drop(&mut self) {
        let bytes = self.buf.as_mut().unwrap().bytes.as_mut_slice();
        bytes[self.num_records_offset..][..std::mem::size_of::<u64>()]
            .copy_from_slice(&(self.num_records as u64).to_le_bytes());
        if bytes.len() >= PREALLOCATED_BUF_SIZE {
            self.logger.queue(self.buf.take().unwrap());
        }
    }
}

struct LoggerInner {
    logger_id: usize,
    file: Mutex<File>,
    current_buf: Mutex<Option<LogBuf>>,
    flush_queue: ArrayQueue<LogBuf>,
    free_bufs_tx: Sender<Vec<u8>>,
    free_bufs_rx: Receiver<Vec<u8>>,
    durable_epoch: AtomicU32,
}

impl LoggerInner {
    fn new(logger_id: usize) -> Self {
        let path = format!("{}/log-{}", DATA_DIR, logger_id);
        let file = File::create(path).unwrap();
        let (free_bufs_tx, free_bufs_rx) = crossbeam_channel::bounded(NUM_BUFS_PER_LOGGER);
        for _ in 0..NUM_BUFS_PER_LOGGER {
            free_bufs_tx
                .try_send(Vec::with_capacity(PREALLOCATED_BUF_SIZE))
                .unwrap();
        }
        Self {
            logger_id,
            file: file.into(),
            current_buf: Default::default(),
            flush_queue: ArrayQueue::new(NUM_BUFS_PER_LOGGER),
            free_bufs_tx,
            free_bufs_rx,
            durable_epoch: Default::default(),
        }
    }

    fn queue(&self, buf: LogBuf, flush_req_tx: &Sender<FlushRequest>) {
        assert!(!buf.bytes.is_empty());
        assert!(buf.min_epoch.get().is_some());
        self.flush_queue.push(buf).unwrap();
        flush_req_tx
            .send(FlushRequest {
                logger_id: self.logger_id,
            })
            .unwrap();
    }

    fn flush(&self) -> std::io::Result<()> {
        // While we are holding the lock, no other writer can pop from
        // flush_queue, but the logger can still push to flush_queue.
        let mut file = self.file.lock();

        if self.flush_queue.is_empty() {
            // The buffers have already been flushed when handling previous
            // flush requests, because we may flush more than one buffer per
            // request.
            return Ok(());
        }

        let mut bufs = Vec::with_capacity(NUM_BUFS_PER_LOGGER);
        let mut min_epoch = OnceCell::new();
        for _ in 0..NUM_BUFS_PER_LOGGER {
            let Some(buf) = self.flush_queue.pop() else {
                break;
            };

            // Epoch must be monotonically increasing.
            let buf_min_epoch = *buf.min_epoch.get().unwrap();
            match min_epoch.get() {
                Some(&min_epoch) => assert!(min_epoch <= buf_min_epoch),
                None => min_epoch.set(buf_min_epoch).unwrap(),
            }

            assert!(!buf.bytes.is_empty());
            bufs.push(buf.bytes);
        }
        let next_durable_epoch = Epoch(min_epoch.take().unwrap().0 - 1);
        assert!(!bufs.is_empty());

        let mut start_buf_index = 0;
        let mut start_offset = 0;
        while start_buf_index < bufs.len() {
            let mut slices = Vec::with_capacity(bufs.len() - start_buf_index);
            slices.push(IoSlice::new(&bufs[start_buf_index][start_offset..]));
            slices.extend(
                bufs[start_buf_index + 1..]
                    .iter()
                    .map(|buf| IoSlice::new(buf)),
            );

            // TODO: Use write_vectored_all once it is stabilized.
            match file.write_vectored(&slices) {
                Ok(0) => return Err(std::io::ErrorKind::WriteZero.into()),
                Ok(n) => {
                    let mut num_bufs_to_remove = 0;
                    let mut left = n;
                    for slice in slices {
                        if let Some(remainder) = left.checked_sub(slice.len()) {
                            left = remainder;
                            num_bufs_to_remove += 1;
                        } else {
                            break;
                        }
                    }
                    for bytes in &mut bufs[start_buf_index..][..num_bufs_to_remove] {
                        let mut bytes = std::mem::take(bytes);
                        bytes.clear();
                        self.free_bufs_tx.try_send(bytes).unwrap();
                    }
                    if num_bufs_to_remove > 0 {
                        start_buf_index += num_bufs_to_remove;
                        start_offset = 0;
                    }
                    if start_buf_index < bufs.len() {
                        start_offset += left;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => (),
                Err(e) => return Err(e),
            }
        }
        assert_eq!(start_buf_index, bufs.len());
        file.sync_data()?;

        let prev_durable_epoch = Epoch(self.durable_epoch.swap(next_durable_epoch.0, SeqCst));
        assert!(prev_durable_epoch <= next_durable_epoch);

        Ok(())
    }
}

#[derive(Debug)]
struct LogBuf {
    bytes: Vec<u8>,
    min_epoch: OnceCell<Epoch>,
}

impl LogBuf {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            min_epoch: Default::default(),
        }
    }
}
