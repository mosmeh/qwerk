use super::{
    file_id::LogFileId,
    fsync_dir,
    io_monitor::{IoMonitor, IoScope},
    WriteBytesCounter,
};
use crate::{
    bytes_ext::{ByteVecExt, BytesExt},
    epoch::{Epoch, EpochFramework},
    signal_channel,
    slotted_cell::{Slot, SlottedCell},
    tid::Tid,
    Error, Result,
};
use crossbeam_queue::ArrayQueue;
use parking_lot::{Condvar, MappedMutexGuard, Mutex, MutexGuard};
use std::{
    fs::{DirBuilder, File},
    io::{IoSlice, Read, Write},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, Ordering::SeqCst},
        Arc,
    },
    thread::JoinHandle,
};

pub struct Config {
    pub dir: PathBuf,
    pub epoch_fw: Arc<EpochFramework>,
    pub persistent_epoch: Arc<PersistentEpoch>,
    pub io_monitor: Arc<IoMonitor>,
    pub flushing_threads: NonZeroUsize,
    pub preallocated_buffer_size: usize,
    pub buffers_per_writer: NonZeroUsize,
    pub max_file_size: usize,
}

/// A redo logger.
pub struct Logger {
    config: Arc<Config>,
    channels: Arc<SlottedCell<LogChannel>>,
    flush_req_tx: Option<crossbeam_channel::Sender<FlushRequest>>,
    flushers: Vec<JoinHandle<()>>,
    daemon: Option<JoinHandle<()>>,
    stop_tx: signal_channel::Sender,
}

impl Logger {
    pub fn new(config: Config) -> Result<Self> {
        DirBuilder::new().recursive(true).create(&config.dir)?;

        let channels = Arc::new(SlottedCell::<LogChannel>::default());

        // flush_req_tx is not a member of LogChannel, because we want to tell
        // flushers to exit by closing the flush_req, but the flushers hold
        // references to LogChannel.
        let (flush_req_tx, flush_req_rx) = crossbeam_channel::unbounded::<FlushRequest>();

        let flushers = (0..config.flushing_threads.get())
            .map(|_| {
                let channels = channels.clone();
                let flush_req_rx = flush_req_rx.clone();
                std::thread::spawn(move || run_flusher(&channels, flush_req_rx))
            })
            .collect();

        let config = Arc::new(config);
        let (stop_tx, stop_rx) = signal_channel::channel();
        let daemon = {
            let config = config.clone();
            let channels = channels.clone();
            let flush_req_tx = flush_req_tx.clone();
            std::thread::Builder::new()
                .name("logger_daemon".into())
                .spawn(move || run_daemon(&config, &channels, flush_req_tx, stop_rx))
                .unwrap()
        };

        Ok(Self {
            config,
            channels,
            flush_req_tx: Some(flush_req_tx),
            flushers,
            daemon: Some(daemon),
            stop_tx,
        })
    }

    pub fn writer(&self) -> Result<LogWriter> {
        Ok(LogWriter {
            logger: self,
            channel: self.channels.try_alloc_with(|index| {
                self.config
                    .io_monitor
                    .do_foreground(|| LogChannel::new(index, self.config.clone()))
            })?,
            flush_req_tx: self.flush_req_tx.clone().unwrap(),
        })
    }

    /// Makes sure that all the logs written before this call are durable.
    ///
    /// Returns the durable epoch after the flush.
    pub fn flush(&self) -> Result<Epoch> {
        self.config.epoch_fw.sync();
        for channel in self.channels.iter() {
            let mut write_state = channel.write_state.lock();
            if let Some(buf) = write_state.take_queueable_buf() {
                channel.queue(buf);
            }

            let mut flush_state = channel.flush_state.lock();
            channel
                .io_scope
                .do_foreground(|| channel.flush(&mut flush_state))?;
            assert!(channel.flush_queue.is_empty());

            // The next log buffer that will be queued to the channel will
            // have an epoch >= global_epoch.
            let global_epoch = self.config.epoch_fw.global_epoch();
            channel
                .durable_epoch
                .fetch_max(global_epoch.decrement().0, SeqCst);
        }
        self.config
            .io_monitor
            .do_foreground(|| self.config.persistent_epoch.update(&self.channels))
    }
}

impl Drop for Logger {
    fn drop(&mut self) {
        self.stop_tx.send();
        self.flush_req_tx.take().unwrap();
        self.daemon.take().unwrap().join().unwrap();
        for flusher in self.flushers.drain(..) {
            flusher.join().unwrap();
        }
        let _ = self.flush();
    }
}

fn run_flusher(
    channels: &SlottedCell<LogChannel>,
    flush_req_rx: crossbeam_channel::Receiver<FlushRequest>,
) {
    while let Ok(req) = flush_req_rx.recv() {
        let channel = channels
            .get(req.channel_index)
            .expect("invalid channel index");
        if channel.flush_queue.is_empty() {
            // The buffers have already been flushed when handling previous
            // flush requests, or are being flushed by other flushers.
            continue;
        }
        if !channel
            .io_scope
            .do_background(|| channel.flush(&mut channel.flush_state.lock()))
        {
            return;
        }
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
    flush_req_tx: crossbeam_channel::Sender<FlushRequest>,
    stop_rx: signal_channel::Receiver,
) {
    for _ in stop_rx.tick(config.epoch_fw.epoch_duration()) {
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
                // The next log buffer that will be queued to the channel will
                // have an epoch >= global_epoch.
                let global_epoch = config.epoch_fw.global_epoch();
                channel
                    .durable_epoch
                    .fetch_max(global_epoch.decrement().0, SeqCst);
            }
        }

        // 3. Update and persist the global durable epoch.
        if !config
            .io_monitor
            .do_background(|| config.persistent_epoch.update(channels).map(|_| ()))
        {
            return;
        }
    }
}

/// Global durable epoch persisted to the disk.
pub struct PersistentEpoch {
    dir: PathBuf,
    path: PathBuf,
    tmp_path: PathBuf,

    /// This mutex also guards the file at `path`.
    durable_epoch: Mutex<Epoch>,

    update_condvar: Condvar,
}

impl PersistentEpoch {
    pub fn new(dir: &Path) -> Result<Self> {
        const FILE_SIZE: usize = std::mem::size_of::<Epoch>();

        let path = dir.join("durable_epoch");
        let epoch = match File::open(&path) {
            Ok(file) if file.metadata()?.len() != FILE_SIZE as u64 => {
                return Err(Error::DatabaseCorrupted)
            }
            Ok(mut file) => {
                let mut bytes = [0; FILE_SIZE];
                file.read_exact(&mut bytes)?;
                u32::from_le_bytes(bytes)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => return Err(e.into()),
        };
        let tmp_path = path.with_extension("tmp");
        Ok(Self {
            dir: dir.into(),
            path,
            tmp_path,
            durable_epoch: Epoch(epoch).into(),
            update_condvar: Default::default(),
        })
    }

    pub fn get(&self) -> Epoch {
        *self.durable_epoch.lock()
    }

    /// Waits until the global durable epoch is equal to or greater than
    /// the given epoch.
    ///
    /// Returns the global durable epoch after the wait.
    pub fn wait_for(&self, epoch: Epoch) -> Epoch {
        let mut durable_epoch = self.durable_epoch.lock();
        self.update_condvar
            .wait_while(&mut durable_epoch, |durable_epoch| *durable_epoch < epoch);
        *durable_epoch
    }

    /// Updates the global durable epoch to the minimum durable epoch of
    /// all channels.
    ///
    /// Returns the new global durable epoch.
    fn update(&self, channels: &SlottedCell<LogChannel>) -> std::io::Result<Epoch> {
        let mut guard = self.durable_epoch.lock();

        let min_epoch = channels.iter().map(LogChannel::durable_epoch).min();

        let prev_durable_epoch = *guard;
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
            file.sync_data()?;
        }

        // Atomically replace the file.
        std::fs::rename(&self.tmp_path, &self.path)?;

        // Persist the rename.
        fsync_dir(&self.dir)?;

        *guard = new_durable_epoch;
        drop(guard);

        self.update_condvar.notify_all();
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
    logger: &'a Logger,
    channel: Slot<'a, LogChannel>,
    flush_req_tx: crossbeam_channel::Sender<FlushRequest>,
}

impl LogWriter<'_> {
    pub fn lock(&self) -> Result<LogWriterLock> {
        let mut state = self.channel.write_state.lock();

        // Perform a dummy operation to report I/O errors early.
        self.channel.io_scope.do_foreground(|| Ok(()))?;

        if state.current_buf.is_none() {
            let bytes = state.free_bufs_rx.recv().unwrap();
            assert!(bytes.is_empty());
            state.current_buf = Some(LogBuf::new(bytes));
        }
        Ok(LogWriterLock {
            writer: self,
            buf: MutexGuard::map(state, |state| &mut state.current_buf),
        })
    }
}

/// A locked reference to the `LogWriter`.
pub struct LogWriterLock<'a> {
    writer: &'a LogWriter<'a>,
    buf: MappedMutexGuard<'a, Option<LogBuf>>,
}

impl<'a> LogWriterLock<'a> {
    pub fn insert_entry(mut self, tid: Tid) -> LogEntry<'a> {
        let buf = self.buf.as_mut().unwrap();

        let epoch = tid.epoch();
        buf.max_epoch = Some(buf.max_epoch.map_or(epoch, |max_epoch| {
            assert!(epoch >= max_epoch);
            max_epoch.max(epoch)
        }));

        buf.bytes.write_u64(tid.0);

        let num_records_offset = buf.bytes.len();
        buf.bytes.write_u64(u64::MAX); // placeholder

        LogEntry {
            writer: self.writer,
            buf: self.buf,
            num_records_offset,
            num_records: 0,
        }
    }
}

pub struct LogEntry<'a> {
    writer: &'a LogWriter<'a>,
    buf: MappedMutexGuard<'a, Option<LogBuf>>,
    num_records_offset: usize,
    num_records: u64,
}

impl LogEntry<'_> {
    /// Writes a key-value pair to the log entry.
    ///
    /// `value` of `None` means tombstone.
    pub fn write(&mut self, key: &[u8], value: Option<&[u8]>) {
        let bytes = &mut self.buf.as_mut().unwrap().bytes;
        bytes.write_bytes(key);
        bytes.write_maybe_bytes(value);
        self.num_records += 1;
    }

    /// Finishes writing the log entry, and persists it to the disk.
    ///
    /// Returns the global durable epoch after the flush.
    pub fn flush(mut self) -> Result<Epoch> {
        let mut buf = self.buf.take().unwrap();
        buf.bytes.set_u64(self.num_records_offset, self.num_records);

        let channel = &*self.writer.channel;
        channel.queue(buf);

        let mut flush_state = channel.flush_state.lock();
        channel.io_scope.do_foreground(|| {
            channel.flush(&mut flush_state)?;
            assert!(channel.flush_queue.is_empty());

            let logger = self.writer.logger;
            let durable_epoch = logger.config.persistent_epoch.update(&logger.channels)?;
            Ok(durable_epoch)
        })
    }
}

impl Drop for LogEntry<'_> {
    /// Queues the flush of the log entry if the buffer is full.
    fn drop(&mut self) {
        let Some(buf) = self.buf.as_mut() else {
            return;
        };
        buf.bytes.set_u64(self.num_records_offset, self.num_records);

        let channel = &*self.writer.channel;
        if buf.bytes.len() >= channel.config.preallocated_buffer_size {
            channel.queue(self.buf.take().unwrap());
            channel.request_flush(&self.writer.flush_req_tx);
        }
    }
}

struct WriteState {
    current_buf: Option<LogBuf>,
    free_bufs_rx: crossbeam_channel::Receiver<Vec<u8>>,
}

impl WriteState {
    fn take_queueable_buf(&mut self) -> Option<LogBuf> {
        if let Some(buf) = &mut self.current_buf {
            if !buf.bytes.is_empty() {
                assert!(buf.max_epoch.is_some());
                return self.current_buf.take();
            }
        }
        None
    }
}

struct FlushState {
    file: Option<WriteBytesCounter<File>>,
    file_path: PathBuf,
    last_archived_epoch: Epoch,
    free_bufs_tx: crossbeam_channel::Sender<Vec<u8>>,
}

pub struct LogChannel {
    index: usize,
    config: Arc<Config>,
    flush_queue: ArrayQueue<LogBuf>,
    durable_epoch: AtomicU32,
    io_scope: IoScope,

    /// This mutex also guards push to `flush_queue`.
    write_state: Mutex<WriteState>,

    /// This mutex also guards
    /// - pop from `flush_queue`
    /// - update of `durable_epoch`
    flush_state: Mutex<FlushState>,
}

impl LogChannel {
    fn new(index: usize, config: Arc<Config>) -> std::io::Result<Self> {
        let file_path = config.dir.join(
            LogFileId::Current {
                channel_index: index,
            }
            .file_name(),
        );
        let file = File::options().append(true).create(true).open(&file_path)?;

        let flush_queue = ArrayQueue::new(config.buffers_per_writer.get());

        // The first log buffer that will be queued to the channel will have
        // an epoch >= global_epoch.
        let durable_epoch = config.epoch_fw.global_epoch().decrement();

        let (free_bufs_tx, free_bufs_rx) =
            crossbeam_channel::bounded(config.buffers_per_writer.get());
        for _ in 0..config.buffers_per_writer.get() {
            free_bufs_tx
                .try_send(Vec::with_capacity(config.preallocated_buffer_size))
                .unwrap();
        }

        let io_scope = IoScope::new(config.io_monitor.clone());

        Ok(Self {
            index,
            config,
            flush_queue,
            durable_epoch: durable_epoch.0.into(),
            io_scope,
            write_state: Mutex::new(WriteState {
                current_buf: Default::default(),
                free_bufs_rx,
            }),
            flush_state: Mutex::new(FlushState {
                file: Some(WriteBytesCounter::new(file)),
                file_path,
                last_archived_epoch: Epoch(0),
                free_bufs_tx,
            }),
        })
    }

    fn durable_epoch(&self) -> Epoch {
        Epoch(self.durable_epoch.load(SeqCst))
    }

    fn queue(&self, buf: LogBuf) {
        assert!(!buf.bytes.is_empty());
        assert!(buf.max_epoch.is_some());
        self.flush_queue.push(buf).unwrap();
    }

    fn request_flush(&self, flush_req_tx: &crossbeam_channel::Sender<FlushRequest>) {
        let _ = flush_req_tx.send(FlushRequest {
            channel_index: self.index,
        });
    }

    fn flush(&self, state: &mut FlushState) -> std::io::Result<()> {
        if self.flush_queue.is_empty() {
            return Ok(());
        }

        assert!(self.flush_queue.len() <= self.config.buffers_per_writer.get());

        let mut bufs_to_flush = Vec::with_capacity(self.config.buffers_per_writer.get());
        let mut max_epoch = Epoch(0);
        while let Some(buf) = self.flush_queue.pop() {
            assert!(!buf.bytes.is_empty());
            bufs_to_flush.push(buf.bytes);
            max_epoch = max_epoch.max(buf.max_epoch.unwrap());
        }

        // Because the epoch queued to the channel is non-decreasing,
        // we are sure that epochs <= max_epoch - 1 will be never queued to the
        // channel. So we can declare that max_epoch - 1 is durable after
        // the flush.
        let next_durable_epoch = max_epoch.decrement();

        assert!(!bufs_to_flush.is_empty());
        assert!(bufs_to_flush.len() <= self.config.buffers_per_writer.get());

        let file = state.file.as_mut().unwrap();
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

            // TODO: Use write_all_vectored once it is stabilized.
            let result = file.write_vectored(&slices);
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

        file.get_ref().sync_data()?;

        let prev_durable_epoch = Epoch(self.durable_epoch.swap(next_durable_epoch.0, SeqCst));
        assert!(prev_durable_epoch <= next_durable_epoch);

        // Roll over the log file if
        // (1) The file is large enough, and
        // (2) The max epoch of the new archived log file will be different from
        //     the max epoch of the previous archived log file.
        // (1) ensures we don't roll over the log file too frequently.
        // (2) ensures the file names of the archived log files are unique.
        if file.num_bytes_written() > self.config.max_file_size
            && max_epoch > state.last_archived_epoch
        {
            // Close the current file.
            state.file.take().unwrap();

            let archive_path = self.config.dir.join(
                LogFileId::Archive {
                    channel_index: self.index,
                    max_epoch,
                }
                .file_name(),
            );
            std::fs::rename(&state.file_path, archive_path)?;

            state.last_archived_epoch = max_epoch;
            state.file = Some(WriteBytesCounter::new(
                File::options()
                    .append(true)
                    .create_new(true)
                    .open(&state.file_path)?,
            ));

            // Persist the rename.
            fsync_dir(&self.config.dir)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
struct LogBuf {
    bytes: Vec<u8>,
    max_epoch: Option<Epoch>,
}

impl LogBuf {
    fn new(bytes: Vec<u8>) -> Self {
        assert!(bytes.is_empty());
        Self {
            bytes,
            max_epoch: Default::default(),
        }
    }
}
