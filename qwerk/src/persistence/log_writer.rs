use crate::{
    epoch::{Epoch, EpochFramework},
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
    io::{IoSlice, Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering::SeqCst},
        Arc,
    },
    thread::JoinHandle,
};

pub struct Config {
    pub dir: PathBuf,
    pub epoch_fw: Arc<EpochFramework>,
    pub persisted_durable_epoch: PersistedDurableEpoch,
    pub flushing_threads: usize,
    pub preallocated_buffer_size: usize,
    pub buffers_per_writer: usize,
    pub fsync: bool,
}

/// A redo logger.
pub struct Logger {
    config: Arc<Config>,
    channels: Arc<SlottedCell<LogChannel>>,
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
                        let channel = channels
                            .get(req.channel_index)
                            .expect("invalid channel index");
                        if channel.flush_queue.is_empty() {
                            // The buffers have already been flushed when
                            // handling previous flush requests,
                            // or are being flushed by other flushers.
                            continue;
                        }
                        let mut state = channel.flush_state.lock();
                        channel.flush(&mut state).unwrap();
                    }
                })
            })
            .collect();

        let config = Arc::new(config);
        let is_running = Arc::new(AtomicBool::new(true));
        let daemon = {
            let config = config.clone();
            let channels = channels.clone();
            let flush_req_tx = flush_req_tx.clone();
            let is_running = is_running.clone();
            std::thread::Builder::new()
                .name("log_system_daemon".into())
                .spawn(move || run_daemon(&config, &channels, flush_req_tx, &is_running))
                .unwrap()
        };

        Ok(Self {
            config,
            channels,
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
                .alloc_with(|index| LogChannel::new(index, self.config.clone()).unwrap()),
            flush_req_tx: self.flush_req_tx.clone().unwrap(),
        }
    }

    pub fn durable_epoch(&self) -> Epoch {
        self.config.persisted_durable_epoch.get()
    }

    /// Makes sure that all the logs written before this call are durable.
    ///
    /// Returns the durable epoch after the flush.
    pub fn flush(&self) -> std::io::Result<Epoch> {
        let reclamation_epoch = self.config.epoch_fw.sync();
        for channel in self.channels.iter() {
            let mut write_state = channel.write_state.lock();
            if let Some(buf) = write_state.take_queueable_buf() {
                channel.queue(buf);
            }

            let mut flush_state = channel.flush_state.lock();
            channel.flush(&mut flush_state)?;
            assert!(channel.flush_queue.is_empty());

            // The next log buffer that will be queued to the channel will
            // have an epoch > reclamation_epoch. Thus, we are sure that
            // durable_epoch >= reclamation_epoch.
            channel.durable_epoch.fetch_max(reclamation_epoch.0, SeqCst);
        }
        self.config
            .persisted_durable_epoch
            .update(&self.channels, self.config.fsync)
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

// The daemon serves the following purposes:
// 1. It periodically queues current_buf to flush_queue.
//    This is needed because the TransactionExecutors may not process further
//    transactions, and in such case the current_buf will never be queued.
//    The daemon ensures that the current_buf is queued at some point.
// 2. It bumps durable_epoch of channels that have no activity, so that they
//    don't drag down the global durable epoch.
// 3. It updates the global durable epoch by calculating the minimum
//    durable epoch of all channels.
fn run_daemon(
    config: &Config,
    channels: &SlottedCell<LogChannel>,
    flush_req_tx: Sender<FlushRequest>,
    is_running: &AtomicBool,
) {
    while is_running.load(SeqCst) {
        for channel in channels.iter() {
            // Failure of this lock means that a writer is writing to
            // the buffer. In that case we can just let the writer
            // queue the buffer by itself if needed.
            let Some(mut write_state) = channel.write_state.try_lock() else {
                continue;
            };

            // 1. Queue current_buf to flush_queue.
            if let Some(buf) = write_state.take_queueable_buf() {
                channel.queue(buf);
                channel.request_flush(&flush_req_tx);
                continue;
            }

            // 2. Bump durable_epoch of channels that have no activity.
            let Some(_flush_state) = channel.flush_state.try_lock() else {
                continue;
            };
            if channel.flush_queue.is_empty() {
                // The next log buffer that will be queued to the channel
                // will have an epoch > reclamation_epoch.
                // Thus, we are sure that durable_epoch is >= reclamation_epoch.
                channel
                    .durable_epoch
                    .fetch_max(config.epoch_fw.reclamation_epoch().0, SeqCst);
            }
        }

        // 3. Update and persist the global durable epoch.
        config
            .persisted_durable_epoch
            .update(channels, config.fsync)
            .unwrap();

        std::thread::sleep(config.epoch_fw.epoch_duration());
    }
}

/// Global durable epoch persisted to the disk.
pub struct PersistedDurableEpoch {
    path: PathBuf,
    tmp_path: PathBuf,
    epoch: AtomicU32,

    /// A mutex guarding modification of `epoch` and the file (`path`).
    mutex: Mutex<()>,
}

impl PersistedDurableEpoch {
    pub fn new(dir: &Path) -> Result<Self> {
        const FILE_SIZE: usize = std::mem::size_of::<Epoch>();

        let path = dir.join("durable_epoch");
        let epoch = match File::open(&path) {
            Ok(file) if file.metadata()?.len() != FILE_SIZE as u64 => return Err(Error::Corrupted),
            Ok(mut file) => {
                let mut bytes = [0; FILE_SIZE];
                file.read_exact(&mut bytes)?;
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

    /// Updates the global durable epoch to the minimum durable epoch of
    /// all channels.
    ///
    /// Returns the new durable epoch.
    fn update(&self, channels: &SlottedCell<LogChannel>, fsync: bool) -> std::io::Result<Epoch> {
        let _guard = self.mutex.lock();

        let mut min_epoch = None;
        for channel in channels.iter() {
            let epoch = channel.durable_epoch();
            if min_epoch.map(|min_epoch| epoch < min_epoch).unwrap_or(true) {
                min_epoch = Some(epoch);
            }
        }

        let prev_durable_epoch = self.get();
        let Some(new_durable_epoch) = min_epoch else {
            return Ok(prev_durable_epoch);
        };

        // This holds because
        // - durable_epochs of channels are non-decreasing
        // - When a new channel is created,
        //   new_durable_epoch >= (the new channel's durable_epoch) (Because we take minimum among all channels)
        //                      = global_epoch - 1                  (See LogChannel::new)
        //                     >= prev_durable_epoch.               (See LogChannel::flush)
        assert!(prev_durable_epoch <= new_durable_epoch);

        if new_durable_epoch == prev_durable_epoch {
            return Ok(new_durable_epoch);
        }

        {
            let mut file = File::create(&self.tmp_path)?;
            file.write_all(&new_durable_epoch.0.to_le_bytes())?;
            if fsync {
                file.sync_data()?;
            }
        }

        // Atomically replace the file.
        std::fs::rename(&self.tmp_path, &self.path)?;
        // TODO: fsync the parent directory.

        self.epoch.store(new_durable_epoch.0, SeqCst);

        Ok(new_durable_epoch)
    }
}

struct FlushRequest {
    channel_index: usize,
}

// Architecture (single channel):
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

    fn queue_and_request_flush(&self, buf: LogBuf) {
        self.channel.queue(buf);
        self.channel.request_flush(&self.flush_req_tx);
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
        let mut state = self.writer.channel.write_state.lock();
        if state
            .current_buf
            .as_ref()
            .map(|buf| !buf.bytes.is_empty() && buf.bytes.len() + self.num_bytes > size)
            .unwrap_or_default()
        {
            self.writer
                .queue_and_request_flush(state.current_buf.take().unwrap());
        }

        if state.current_buf.is_none() {
            let bytes = state.free_bufs_rx.recv().unwrap();
            assert!(bytes.is_empty());
            state.current_buf = Some(LogBuf::new(bytes));
        }

        // The buffer can exceed the preallocated size if the single transaction
        // is too large.
        state
            .current_buf
            .as_mut()
            .unwrap()
            .bytes
            .reserve(self.num_bytes);

        ReservedCapacity {
            writer: self.writer,
            buf: MutexGuard::map(state, |state| &mut state.current_buf),
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
            self.writer
                .queue_and_request_flush(self.buf.take().unwrap());
        }
    }
}

struct WriteState {
    current_buf: Option<LogBuf>,
    free_bufs_rx: Receiver<Vec<u8>>,
}

impl WriteState {
    fn take_queueable_buf(&mut self) -> Option<LogBuf> {
        if let Some(buf) = &mut self.current_buf {
            if !buf.bytes.is_empty() {
                assert!(buf.min_epoch.get().is_some());
                return self.current_buf.take();
            }
        }
        None
    }
}

struct FlushState {
    file: File,
    free_bufs_tx: Sender<Vec<u8>>,
}

pub struct LogChannel {
    index: usize,
    config: Arc<Config>,
    flush_queue: ArrayQueue<LogBuf>,
    durable_epoch: AtomicU32,

    /// This mutex also guards push to `flush_queue`.
    write_state: Mutex<WriteState>,

    /// This mutex also guards
    /// - pop from `flush_queue`
    /// - update of `durable_epoch`
    flush_state: Mutex<FlushState>,
}

impl LogChannel {
    fn new(index: usize, config: Arc<Config>) -> std::io::Result<Self> {
        let path = config
            .dir
            .join(format!("{}{}", super::LOG_FILE_NAME_PREFIX, index));
        let file = File::create(path)?;

        let flush_queue = ArrayQueue::new(config.buffers_per_writer);

        // The first log buffer that will be queued to the channel will have
        // an epoch >= global_epoch.
        let durable_epoch = config.epoch_fw.global_epoch().decrement();

        let (free_bufs_tx, free_bufs_rx) = crossbeam_channel::bounded(config.buffers_per_writer);
        for _ in 0..config.buffers_per_writer {
            free_bufs_tx
                .try_send(Vec::with_capacity(config.preallocated_buffer_size))
                .unwrap();
        }

        Ok(Self {
            index,
            config,
            flush_queue,
            durable_epoch: durable_epoch.0.into(),
            write_state: Mutex::new(WriteState {
                current_buf: Default::default(),
                free_bufs_rx,
            }),
            flush_state: Mutex::new(FlushState { file, free_bufs_tx }),
        })
    }

    fn durable_epoch(&self) -> Epoch {
        Epoch(self.durable_epoch.load(SeqCst))
    }

    fn queue(&self, buf: LogBuf) {
        assert!(!buf.bytes.is_empty());
        assert!(buf.min_epoch.get().is_some());
        self.flush_queue.push(buf).unwrap();
    }

    fn request_flush(&self, flush_req_tx: &Sender<FlushRequest>) {
        flush_req_tx
            .send(FlushRequest {
                channel_index: self.index,
            })
            .unwrap();
    }

    fn flush(&self, state: &mut FlushState) -> std::io::Result<()> {
        if self.flush_queue.is_empty() {
            return Ok(());
        }

        assert!(self.flush_queue.len() <= self.config.buffers_per_writer);

        let mut bufs_to_flush = Vec::with_capacity(self.config.buffers_per_writer);
        let min_epoch = OnceCell::new();
        while let Some(buf) = self.flush_queue.pop() {
            // Epoch is non-decreasing, so the first buffer has
            // the smallest epoch.
            let buf_min_epoch = *buf.min_epoch.get().unwrap();
            let min_epoch = min_epoch.get_or_init(|| buf_min_epoch);
            assert!(buf_min_epoch >= *min_epoch);

            assert!(!buf.bytes.is_empty());
            bufs_to_flush.push(buf.bytes);
        }

        // Because the epoch queued to the channel is non-decreasing,
        // we are sure that epochs <= min_epoch - 1 will be never queued to the
        // channel. So we can declare that min_epoch - 1 is durable after
        // the flush.
        let next_durable_epoch = min_epoch.get().unwrap().decrement();

        assert!(!bufs_to_flush.is_empty());
        assert!(bufs_to_flush.len() <= self.config.buffers_per_writer);

        let mut start_buf_index = 0;
        let mut start_byte_offset = 0;
        while start_buf_index < bufs_to_flush.len() {
            let mut slices = Vec::with_capacity(bufs_to_flush.len() - start_buf_index);
            slices.push(IoSlice::new(
                &bufs_to_flush[start_buf_index][start_byte_offset..],
            ));
            slices.extend(
                bufs_to_flush[start_buf_index + 1..]
                    .iter()
                    .map(|buf| IoSlice::new(buf)),
            );

            // TODO: Use write_vectored_all once it is stabilized.
            let result = state.file.write_vectored(&slices);
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
                        state.free_bufs_tx.try_send(bytes).unwrap();
                    }
                    if num_bufs_to_remove > 0 {
                        start_buf_index += num_bufs_to_remove;
                        start_byte_offset = 0;
                    }
                    if start_buf_index < bufs_to_flush.len() {
                        start_byte_offset += left;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => (),
                Err(e) => return Err(e),
            }
        }
        assert_eq!(start_buf_index, bufs_to_flush.len());

        if self.config.fsync {
            state.file.sync_data()?;
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
        assert!(bytes.is_empty());
        Self {
            bytes,
            min_epoch: Default::default(),
        }
    }
}
