use crate::{
    epoch::Epoch,
    slotted_cell::{Slot, SlottedCell},
    tid::Tid,
    Error, Result,
};
use crossbeam_channel::{Receiver, Sender};
use crossbeam_queue::ArrayQueue;
use parking_lot::{MappedMutexGuard, Mutex, MutexGuard};
use std::{
    cell::OnceCell,
    fs::{DirBuilder, File},
    io::{IoSlice, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering::SeqCst},
        Arc,
    },
    thread::JoinHandle,
    time::Duration,
};

pub struct Config {
    pub dir: PathBuf,
    pub flushing_threads: usize,
    pub preallocated_buffer_size: usize,
    pub buffers_per_writer: usize,
    pub fsync: bool,
}

/// A redo logger.
pub struct Logger {
    config: Arc<Config>,
    channels: Arc<SlottedCell<LogChannel>>,
    durable_epoch: Arc<DurableEpoch>,
    flush_req_tx: Option<Sender<FlushRequest>>,
    flushers: Vec<JoinHandle<()>>,
    daemon: Option<JoinHandle<()>>,
    is_running: Arc<AtomicBool>,
}

impl Logger {
    pub fn new(config: Config) -> Result<Self> {
        DirBuilder::new().recursive(true).create(&config.dir)?;

        let channels = Arc::new(SlottedCell::<LogChannel>::default());

        // flush_req is not a member of LogChannel, because we want to tell
        // flushers to exit by closing the channel, but the flushers hold
        // references to LogChannel.
        let (flush_req_tx, flush_req_rx) = crossbeam_channel::unbounded::<FlushRequest>();

        let flushers = (0..config.flushing_threads)
            .map(|_| {
                let channels = channels.clone();
                let flush_req_rx = flush_req_rx.clone();
                std::thread::spawn(move || {
                    while let Ok(req) = flush_req_rx.recv() {
                        channels.get(req.writer_id).unwrap().flush().unwrap();
                    }
                })
            })
            .collect();

        let durable_epoch = Arc::new(DurableEpoch::new(&config.dir)?);
        let is_running = Arc::new(AtomicBool::new(true));
        let daemon = {
            let channels = channels.clone();
            let durable_epoch = durable_epoch.clone();
            let flush_req_tx = flush_req_tx.clone();
            let is_running = is_running.clone();
            std::thread::Builder::new()
                .name("log_system_daemon".into())
                .spawn(move || run_daemon(&channels, &durable_epoch, flush_req_tx, &is_running))
                .unwrap()
        };

        Ok(Self {
            config: config.into(),
            channels,
            durable_epoch,
            flush_req_tx: Some(flush_req_tx),
            flushers,
            daemon: Some(daemon),
            is_running,
        })
    }

    pub fn spawn_writer(&self) -> LogWriter {
        LogWriter {
            channel: self
                .channels
                .alloc_with(|writer_id| LogChannel::new(writer_id, self.config.clone())),
            flush_req_tx: self.flush_req_tx.clone().unwrap(),
        }
    }

    pub fn durable_epoch(&self) -> Epoch {
        self.durable_epoch.get()
    }

    pub fn flush(&self) -> std::io::Result<()> {
        for channel in self.channels.iter() {
            {
                let mut producer = channel.producer.lock();
                let buf = &mut producer.current_buf;
                if buf
                    .as_ref()
                    .map(|buf| !buf.bytes.is_empty())
                    .unwrap_or_default()
                {
                    // We don't use channel.queue() here, because we flush
                    // the buffer by ourselves and don't need to request
                    // a flush.
                    channel.flush_queue.push(buf.take().unwrap()).unwrap();
                }
            }
            channel.flush()?;
        }
        self.durable_epoch.update(&self.channels)?;
        Ok(())
    }
}

impl Drop for Logger {
    fn drop(&mut self) {
        self.is_running.store(false, SeqCst);
        self.flush_req_tx.take().unwrap();
        self.daemon.take().unwrap().join().unwrap();
        for flusher in self.flushers.drain(..) {
            flusher.join().unwrap();
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
//    durable epoch of all writers.
fn run_daemon(
    channels: &SlottedCell<LogChannel>,
    durable_epoch: &DurableEpoch,
    flush_req_tx: Sender<FlushRequest>,
    is_running: &AtomicBool,
) {
    while is_running.load(SeqCst) {
        for channel in channels.iter() {
            // Failure of this lock means that a writer is writing to
            // the buffer. In that case we can just let the writer
            // queue the buffer by itself if needed.
            let Some(mut producer) = channel.producer.try_lock() else {
                continue;
            };
            let buf = &mut producer.current_buf;
            if buf
                .as_ref()
                .map(|buf| !buf.bytes.is_empty())
                .unwrap_or_default()
            {
                channel.queue(buf.take().unwrap(), &flush_req_tx);
            }
        }
        durable_epoch.update(channels).unwrap();
        std::thread::sleep(Duration::from_millis(40));
    }
}

pub struct DurableEpoch {
    path: PathBuf,
    tmp_path: PathBuf,
    epoch: AtomicU32,
    mutex: Mutex<()>,
}

impl DurableEpoch {
    pub fn new(dir: &Path) -> Result<Self> {
        let path = dir.join("durable_epoch");
        let epoch = match std::fs::read(&path) {
            Ok(bytes) => {
                let bytes = bytes.try_into().map_err(|_| Error::Corrupted)?;
                u32::from_le_bytes(bytes)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => return Err(e.into()),
        };
        Ok(Self {
            path,
            tmp_path: dir.join("durable_epoch.tmp"),
            epoch: epoch.into(),
            mutex: Default::default(),
        })
    }

    pub fn get(&self) -> Epoch {
        Epoch(self.epoch.load(SeqCst))
    }

    fn update(&self, channels: &SlottedCell<LogChannel>) -> std::io::Result<()> {
        let _guard = self.mutex.lock();

        let mut min_epoch = None;
        for channel in channels.iter() {
            let Some(durable_epoch) = channel.durable_epoch() else {
                // This channel has not flushed anything yet.
                // We would underestimate the durable epoch if we skip
                // this channel, so we don't update the durable epoch.
                return Ok(());
            };
            match min_epoch {
                Some(min_epoch) if min_epoch <= durable_epoch => (),
                _ => min_epoch = Some(durable_epoch),
            }
        }
        let Some(new_epoch) = min_epoch else {
            return Ok(());
        };

        {
            let mut tmp_pepoch = File::create(&self.tmp_path)?;
            tmp_pepoch.write_all(&new_epoch.0.to_le_bytes())?;
            tmp_pepoch.sync_data()?;
        }

        // Atomically replace the file.
        std::fs::rename(&self.tmp_path, &self.path)?;
        // TODO: fsync the parent directory.

        let prev_epoch = Epoch(self.epoch.swap(new_epoch.0, SeqCst));
        assert!(prev_epoch <= new_epoch);

        Ok(())
    }
}

struct FlushRequest {
    writer_id: usize,
}

// Architecture (single writer/channel):
//                                  +---- flush_req ----+
//                                  |                   |
//                                  |                   v
//            write               queue               flush
// LogWriter ------> current_buf ------> flush_queue ------> file
//                        ^                    |        |
//                        |                    |        v
//                        +---- free_bufs <----+   durable_epoch

pub struct LogWriter<'a> {
    channel: Slot<'a, LogChannel>,
    flush_req_tx: Sender<FlushRequest>,
}

impl LogWriter<'_> {
    pub fn reserver(&mut self) -> CapacityReserver {
        CapacityReserver {
            writer: self,
            num_bytes: std::mem::size_of::<u64>() * 2, // tid and num_records
        }
    }

    fn queue(&self, buf: LogBuf) {
        self.channel.queue(buf, &self.flush_req_tx);
    }
}

pub struct CapacityReserver<'a> {
    writer: &'a LogWriter<'a>,
    num_bytes: usize,
}

impl<'a> CapacityReserver<'a> {
    pub fn reserve_write(&mut self, key: &[u8], value: Option<&[u8]>) {
        self.num_bytes += key.len() + std::mem::size_of::<u64>() * 2; // key, key.len(), and value.len()
        if let Some(value) = value {
            self.num_bytes += value.len();
        }
    }

    pub fn finish(self) -> ReservedCapacity<'a> {
        let size = self.writer.channel.config.preallocated_buffer_size;
        let mut producer = self.writer.channel.producer.lock();
        if producer
            .current_buf
            .as_ref()
            .map(|buf| !buf.bytes.is_empty() && buf.bytes.len() + self.num_bytes > size)
            .unwrap_or_default()
        {
            self.writer.queue(producer.current_buf.take().unwrap());
        }

        if producer.current_buf.is_none() {
            let bytes = producer.free_bufs_rx.recv().unwrap();
            assert!(bytes.is_empty());
            producer.current_buf = Some(LogBuf::new(bytes));
        }

        // The buffer can exceed the preallocated size if the single transaction
        // is too large.
        producer
            .current_buf
            .as_mut()
            .unwrap()
            .bytes
            .reserve(self.num_bytes);

        ReservedCapacity {
            writer: self.writer,
            buf: MutexGuard::map(producer, |producer| &mut producer.current_buf),
        }
    }
}

pub struct ReservedCapacity<'a> {
    writer: &'a LogWriter<'a>,
    buf: MappedMutexGuard<'a, Option<LogBuf>>,
}

impl<'a> ReservedCapacity<'a> {
    pub fn insert(mut self, tid: Tid) -> Entry<'a> {
        let buf = self.buf.as_mut().unwrap();
        buf.bytes.extend_from_slice(&tid.0.to_le_bytes());

        let num_records_offset = buf.bytes.len();
        buf.bytes.extend_from_slice(&u64::MAX.to_le_bytes()); // placeholder

        // Epoch must be monotonically increasing.
        let epoch = tid.epoch();
        let min_epoch = *buf.min_epoch.get_or_init(|| epoch);
        assert!(min_epoch <= epoch);

        Entry {
            writer: self.writer,
            buf: self.buf,
            num_records_offset,
            num_records: 0,
        }
    }
}

pub struct Entry<'a> {
    writer: &'a LogWriter<'a>,
    buf: MappedMutexGuard<'a, Option<LogBuf>>,
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
        if bytes.len() >= self.writer.channel.config.preallocated_buffer_size {
            self.writer.queue(self.buf.take().unwrap());
        }
    }
}

struct Producer {
    current_buf: Option<LogBuf>,
    free_bufs_rx: Receiver<Vec<u8>>,
}

struct Consumer {
    file: File,
    free_bufs_tx: Sender<Vec<u8>>,
}

pub struct LogChannel {
    writer_id: usize,
    config: Arc<Config>,
    flush_queue: ArrayQueue<LogBuf>,
    durable_epoch: AtomicU32,
    producer: Mutex<Producer>,
    consumer: Mutex<Consumer>,
}

impl LogChannel {
    fn new(writer_id: usize, config: Arc<Config>) -> Self {
        let path = config
            .dir
            .join(format!("{}{}", super::LOG_FILE_NAME_PREFIX, writer_id));
        let file = File::create(path).unwrap();
        let flush_queue = ArrayQueue::new(config.buffers_per_writer);
        let (free_bufs_tx, free_bufs_rx) = crossbeam_channel::bounded(config.buffers_per_writer);
        for _ in 0..config.buffers_per_writer {
            free_bufs_tx
                .try_send(Vec::with_capacity(config.preallocated_buffer_size))
                .unwrap();
        }
        Self {
            writer_id,
            config,
            flush_queue,
            durable_epoch: Default::default(),
            producer: Mutex::new(Producer {
                current_buf: Default::default(),
                free_bufs_rx,
            }),
            consumer: Mutex::new(Consumer { file, free_bufs_tx }),
        }
    }

    fn durable_epoch(&self) -> Option<Epoch> {
        // durable_epoch is zero if no flush has happened yet.
        let epoch = self.durable_epoch.load(SeqCst);
        if epoch > 0 {
            Some(Epoch(epoch))
        } else {
            None
        }
    }

    fn queue(&self, buf: LogBuf, flush_req_tx: &Sender<FlushRequest>) {
        assert!(!buf.bytes.is_empty());
        assert!(buf.min_epoch.get().is_some());
        self.flush_queue.push(buf).unwrap();
        flush_req_tx
            .send(FlushRequest {
                writer_id: self.writer_id,
            })
            .unwrap();
    }

    fn flush(&self) -> std::io::Result<()> {
        if self.flush_queue.is_empty() {
            // The buffers have already been flushed when handling previous
            // flush requests, or are being flushed by other flushers.
            return Ok(());
        }

        // While we are holding the lock, no other flusher can pop from
        // flush_queue, but the writer can still push to flush_queue.
        let mut consumer = self.consumer.lock();

        // Checking again, because buffers may have been flushed while we are
        // waiting for the lock.
        if self.flush_queue.is_empty() {
            return Ok(());
        }

        assert!(self.flush_queue.len() <= self.config.buffers_per_writer);

        let mut bufs_to_flush = Vec::with_capacity(self.config.buffers_per_writer);
        let mut min_epoch = OnceCell::new();
        while let Some(buf) = self.flush_queue.pop() {
            // Epoch must be monotonically increasing.
            let buf_min_epoch = *buf.min_epoch.get().unwrap();
            match min_epoch.get() {
                Some(&min_epoch) => assert!(min_epoch <= buf_min_epoch),
                None => min_epoch.set(buf_min_epoch).unwrap(),
            }

            assert!(!buf.bytes.is_empty());
            bufs_to_flush.push(buf.bytes);
        }
        let next_durable_epoch = Epoch(min_epoch.take().unwrap().0 - 1);
        assert!(!bufs_to_flush.is_empty());
        assert!(bufs_to_flush.len() <= self.config.buffers_per_writer);

        let mut start_buf_index = 0;
        let mut start_offset = 0;
        while start_buf_index < bufs_to_flush.len() {
            let mut slices = Vec::with_capacity(bufs_to_flush.len() - start_buf_index);
            slices.push(IoSlice::new(
                &bufs_to_flush[start_buf_index][start_offset..],
            ));
            slices.extend(
                bufs_to_flush[start_buf_index + 1..]
                    .iter()
                    .map(|buf| IoSlice::new(buf)),
            );

            // TODO: Use write_vectored_all once it is stabilized.
            let result = consumer.file.write_vectored(&slices);
            match result {
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
                    for bytes in &mut bufs_to_flush[start_buf_index..][..num_bufs_to_remove] {
                        let mut bytes = std::mem::take(bytes);
                        bytes.clear();
                        consumer.free_bufs_tx.try_send(bytes).unwrap();
                    }
                    if num_bufs_to_remove > 0 {
                        start_buf_index += num_bufs_to_remove;
                        start_offset = 0;
                    }
                    if start_buf_index < bufs_to_flush.len() {
                        start_offset += left;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => (),
                Err(e) => return Err(e),
            }
        }
        assert_eq!(start_buf_index, bufs_to_flush.len());
        if self.config.fsync {
            consumer.file.sync_data()?;
        }

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
