use super::{ConcurrencyControl, ConcurrencyControlInternal, Shared, TransactionExecutor};
use crate::{
    epoch::{Epoch, EpochGuard},
    persistence::LogWriter,
    qsbr::{Qsbr, QsbrGuard},
    record,
    small_bytes::SmallBytes,
    tid::{Tid, TidGenerator},
    Error, Index, Result,
};
use crossbeam_utils::Backoff;
use scc::hash_index::Entry;
use std::{
    cell::OnceCell,
    cmp::Ordering,
    collections::VecDeque,
    sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering::SeqCst},
};

/// Optimistic concurrency control.
///
/// This is an implementation of [Silo](https://doi.org/10.1145/2517349.2522713).
pub struct Optimistic {
    qsbr: Qsbr,
    gc_threshold: usize,
}

impl ConcurrencyControl for Optimistic {}

impl ConcurrencyControlInternal for Optimistic {
    type Record = Record;
    type Executor<'a> = Executor<'a>;

    fn init(gc_threshold: usize) -> Self {
        Self {
            qsbr: Default::default(),
            gc_threshold,
        }
    }

    fn load_log_entry(
        index: &Index<Self::Record>,
        key: SmallBytes,
        value: Option<Box<[u8]>>,
        tid: Tid,
    ) {
        assert!(!tid.has_flags());
        match index.entry(key) {
            Entry::Occupied(entry) => {
                let record_ptr = *entry.get();
                let record = unsafe { record_ptr.as_ref() };
                let record_tid = record
                    .lock_if_present()
                    .expect("the record must be present");
                assert!(!record_tid.has_flags());
                match record_tid.cmp(&tid) {
                    Ordering::Less => {
                        let (buf_ptr, len) = value.into_raw_parts();
                        record.buf_ptr.store(buf_ptr, SeqCst);
                        record.len.store(len, SeqCst);
                        record.tid.store(tid.0, SeqCst); // unlock
                    }
                    Ordering::Equal => unreachable!(),
                    Ordering::Greater => record.unlock(),
                }
            }
            Entry::Vacant(entry) => {
                let (buf_ptr, len) = value.into_raw_parts();
                let record_ptr = Shared::new(Record {
                    buf_ptr: AtomicPtr::new(buf_ptr),
                    len: len.into(),
                    tid: tid.0.into(),
                });
                entry.insert_entry(record_ptr);
            }
        }
    }

    fn spawn_executor<'a>(
        &'a self,
        index: &'a Index<Self::Record>,
        epoch_guard: EpochGuard<'a>,
    ) -> Self::Executor<'a> {
        Self::Executor {
            index,
            global: self,
            epoch_guard,
            tid_generator: Default::default(),
            removal_queue: Default::default(),
            garbage_bytes: 0,
            garbage_values: Default::default(),
            garbage_records: Default::default(),
            qsbr_guard: self.qsbr.acquire(),
            read_set: Default::default(),
            write_set: Default::default(),
        }
    }
}

pub struct Record {
    buf_ptr: AtomicPtr<u8>,
    len: AtomicUsize,
    tid: AtomicU64,
}

impl Drop for Record {
    fn drop(&mut self) {
        let ptr = *self.buf_ptr.get_mut();
        if !ptr.is_null() {
            let len = *self.len.get_mut();
            let _ = unsafe { Box::from_raw(std::slice::from_raw_parts_mut(ptr, len)) };
        }
    }
}

impl Record {
    /// Locks the record if it is present.
    ///
    /// When successful, returns the TID of the record before locking.
    /// Otherwise (i.e. when the record is absent), returns `None`.
    fn lock_if_present(&self) -> Option<Tid> {
        let backoff = Backoff::new();
        loop {
            let current_tid = Tid(self.tid.load(SeqCst));
            if current_tid.is_absent() {
                return None;
            }
            if current_tid.is_locked() {
                backoff.snooze();
                continue;
            }

            let result = self.tid.compare_exchange_weak(
                current_tid.0,
                current_tid.with_locked().0,
                SeqCst,
                SeqCst,
            );
            if result.is_ok() {
                return Some(current_tid);
            }
            backoff.spin();
        }
    }

    /// Unlocks the record.
    ///
    /// # Panics
    ///
    /// Panics if the record is not locked.
    fn unlock(&self) {
        let prev_tid = self.tid.fetch_and(!Tid::LOCKED, SeqCst);
        assert!(Tid(prev_tid).is_locked());
    }

    /// Optimistically reads the record.
    ///
    /// If the record is absent, returns `None`.
    fn read(&self) -> Option<RecordSnapshot> {
        let backoff = Backoff::new();
        loop {
            let tid1 = Tid(self.tid.load(SeqCst));
            if tid1.is_absent() {
                return None;
            }
            if tid1.is_locked() {
                backoff.snooze();
                continue;
            }

            let buf_ptr = self.buf_ptr.load(SeqCst);
            let len = self.len.load(SeqCst);

            let tid2 = Tid(self.tid.load(SeqCst));
            if tid1 == tid2 {
                let value = (!buf_ptr.is_null())
                    .then(|| unsafe { std::slice::from_raw_parts(buf_ptr, len) });
                return Some(RecordSnapshot { value, tid: tid2 });
            }
            backoff.spin();
        }
    }
}

impl record::Record for Record {
    fn is_tombstone(&self) -> bool {
        self.buf_ptr.load(SeqCst).is_null()
    }
}

struct RecordSnapshot<'a> {
    value: Option<&'a [u8]>,
    tid: Tid,
}

pub struct Executor<'a> {
    // Global state
    index: &'a Index<Record>,
    global: &'a Optimistic,

    // Per-executor state
    epoch_guard: EpochGuard<'a>,
    tid_generator: TidGenerator,
    removal_queue: VecDeque<(SmallBytes, Tid)>,
    garbage_bytes: usize,
    garbage_values: Vec<Box<[u8]>>,
    garbage_records: Vec<Shared<Record>>,

    // Per-transaction state
    qsbr_guard: QsbrGuard<'a>,
    read_set: Vec<ReadItem<'a>>,
    write_set: Vec<WriteItem>,
}

impl TransactionExecutor for Executor<'_> {
    fn begin_transaction(&mut self) {
        self.read_set.clear();
        self.write_set.clear();
        self.qsbr_guard.quiesce();
        self.epoch_guard.refresh();
    }

    fn end_transaction(&mut self) {
        self.epoch_guard.release();
        self.qsbr_guard.mark_as_offline();
        self.process_removal_queue();
        if self.garbage_bytes >= self.global.gc_threshold {
            self.collect_garbage();
        }
    }

    fn read(&mut self, key: &[u8]) -> Result<Option<&[u8]>> {
        // Ensures read-your-writes.
        let item = self.write_set.iter().find(|item| item.key.as_ref() == key);
        if let Some(item) = item {
            return Ok(item.value.as_deref());
        }

        // Ensures repeatable reads.
        let item = self.read_set.iter().find(|item| item.key.as_ref() == key);
        if let Some(item) = item {
            return Ok(item.value);
        }

        let key = key.into();
        let record_ptr = self.index.peek_with(&key, |_, value| *value);
        let item = match record_ptr {
            Some(record_ptr) => {
                let Some(RecordSnapshot { value, tid }) = unsafe { record_ptr.as_ref() }.read()
                else {
                    // The record was removed by another transaction.
                    return Err(Error::NotSerializable);
                };
                ReadItem {
                    key,
                    value,
                    record_ptr: Some(record_ptr),
                    tid,
                }
            }
            None => ReadItem {
                key,
                value: None,
                record_ptr: None,
                tid: Tid::ZERO,
            },
        };

        let value = item.value;
        self.read_set.push(item);
        Ok(value)
    }

    fn write(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        let item = self
            .read_set
            .iter_mut()
            .find(|item| item.key.as_ref() == key);
        if let Some(item) = item {
            item.tid = item.tid.with_locked();
        }

        let value = value.map(|value| value.to_vec().into());
        let item = self
            .write_set
            .iter_mut()
            .find(|item| item.key.as_ref() == key);
        if let Some(item) = item {
            item.value = value;
            return Ok(());
        }

        self.write_set.push(WriteItem {
            key: key.into(),
            value,
            target: Default::default(),
        });
        Ok(())
    }

    fn commit(&mut self, log_writer: &mut LogWriter) -> Result<Epoch> {
        let mut log_capacity_reserver = log_writer.reserver();
        for item in &self.write_set {
            log_capacity_reserver.reserve_write(&item.key, item.value.as_deref());
        }
        let reserved_log_capacity = log_capacity_reserver.finish();

        // Phase 1

        // The stability of the sort doesn't matter because the keys are unique.
        self.write_set.sort_unstable_by(|a, b| a.key.cmp(&b.key));

        let mut tid_set = self.tid_generator.begin_transaction();
        for item in &mut self.write_set {
            let (record_ptr, was_vacant) = match self.index.entry(item.key.clone()) {
                Entry::Occupied(entry) => (*entry.get(), false),
                Entry::Vacant(entry) => {
                    let record_ptr = Shared::new(Record {
                        buf_ptr: AtomicPtr::new(std::ptr::null_mut()),
                        len: 0.into(),
                        tid: Tid::ZERO.with_locked().0.into(),
                    });
                    entry.insert_entry(record_ptr);
                    (record_ptr, true)
                }
            };
            if !was_vacant {
                let Some(tid) = unsafe { record_ptr.as_ref() }.lock_if_present() else {
                    // The record was removed by another transaction.
                    return Err(Error::NotSerializable);
                };
                tid_set.add(tid);
            }
            item.target
                .set(WriteTarget {
                    record_ptr,
                    was_vacant,
                })
                .expect("item.target should be set only once");
        }

        // Serialization point
        let commit_epoch = self.epoch_guard.refresh();

        // Phase 2: validation phase
        for item in &self.read_set {
            let record_ptr = match item.record_ptr {
                Some(record_ptr) => record_ptr,
                None if item.tid.is_locked() => {
                    // This item is also in the write set.
                    self.index
                        .peek_with(&item.key, |_, value| *value)
                        .expect("the key must have been ensured to exist in Phase 1")
                }
                None => {
                    // This item is not in the write set.
                    if self.index.contains(&item.key) {
                        // The record was inserted by another transaction.
                        return Err(Error::NotSerializable);
                    }
                    continue;
                }
            };

            let tid = Tid(unsafe { record_ptr.as_ref() }.tid.load(SeqCst));
            if tid != item.tid {
                // The TID doesn't match or the record is locked by
                // another transaction.
                return Err(Error::NotSerializable);
            }
            assert!(!tid.is_absent());

            tid_set.add(tid);
        }

        let commit_tid = tid_set
            .generate_tid(commit_epoch)
            .ok_or(Error::TooManyTransactions)?;

        // Phase 3: write phase
        let mut log_entry = reserved_log_capacity.insert(commit_tid);
        for item in self.write_set.drain(..) {
            log_entry.write(item.key.as_ref(), item.value.as_deref());

            let record_ptr = item
                .target
                .get()
                .expect("item.target must have been set in Phase 1")
                .record_ptr;
            let record = unsafe { record_ptr.as_ref() };

            let (new_buf_ptr, new_len) = item.value.into_raw_parts();
            let prev_buf_ptr = record.buf_ptr.swap(new_buf_ptr, SeqCst);
            let prev_len = record.len.swap(new_len, SeqCst);

            // Store new TID and unlock the record.
            record.tid.store(commit_tid.0, SeqCst);

            if !prev_buf_ptr.is_null() {
                self.garbage_values.push(unsafe {
                    Box::from_raw(std::slice::from_raw_parts_mut(prev_buf_ptr, prev_len))
                });
                self.garbage_bytes += prev_len;
            }
            if new_buf_ptr.is_null() {
                self.removal_queue.push_back((item.key, commit_tid));
            }
        }

        Ok(commit_epoch)
    }

    fn abort(&mut self) {
        for item in &self.write_set {
            let Some(target) = item.target.get() else {
                continue;
            };

            let record = unsafe { target.record_ptr.as_ref() };
            let tid = Tid(record.tid.load(SeqCst));
            assert!(tid.is_locked());

            let mut new_tid = tid.without_locked();
            if target.was_vacant {
                let was_removed = self.index.remove(&item.key);
                assert!(was_removed);
                new_tid = new_tid.with_absent();
            }
            record.tid.store(new_tid.0, SeqCst); // unlock

            if target.was_vacant {
                self.garbage_records.push(target.record_ptr);
                self.garbage_bytes += std::mem::size_of::<Record>();
            }
        }
    }
}

impl Executor<'_> {
    fn process_removal_queue(&mut self) {
        let reclamation_epoch = self.epoch_guard.reclamation_epoch();
        while let Some((_, tid)) = self.removal_queue.front() {
            if tid.epoch() > reclamation_epoch {
                break;
            }
            let (key, tid) = self.removal_queue.pop_front().unwrap();
            self.qsbr_guard.quiesce();
            self.index.remove_if(&key, |record_ptr| {
                let record = unsafe { record_ptr.as_ref() };
                let Some(current_tid) = record.lock_if_present() else {
                    return false;
                };
                assert!(!current_tid.has_flags());
                if current_tid != tid {
                    record.unlock();
                    return false;
                }
                assert!(record.buf_ptr.load(SeqCst).is_null());
                assert_eq!(record.len.load(SeqCst), 0);
                record.tid.store(current_tid.with_absent().0, SeqCst); // unlock
                self.garbage_records.push(*record_ptr);
                self.garbage_bytes += std::mem::size_of::<Record>();
                true
            });
            self.qsbr_guard.mark_as_offline();
        }
    }

    fn collect_garbage(&mut self) {
        assert!(!self.qsbr_guard.is_online());
        self.global.qsbr.sync();
        self.garbage_values.clear();
        for record_ptr in self.garbage_records.drain(..) {
            unsafe { record_ptr.drop_in_place() };
        }
        self.garbage_bytes = 0;
    }
}

impl Drop for Executor<'_> {
    fn drop(&mut self) {
        let backoff = Backoff::new();
        while !self.removal_queue.is_empty() {
            self.process_removal_queue();
            backoff.snooze();
        }
        if !self.garbage_values.is_empty() || !self.garbage_records.is_empty() {
            self.collect_garbage();
        }
    }
}

struct ReadItem<'a> {
    key: SmallBytes,
    value: Option<&'a [u8]>,
    record_ptr: Option<Shared<Record>>,
    tid: Tid,
}

struct WriteItem {
    key: SmallBytes,
    value: Option<Box<[u8]>>,
    target: OnceCell<WriteTarget>,
}

#[derive(Debug)]
struct WriteTarget {
    record_ptr: Shared<Record>,
    was_vacant: bool,
}

impl Tid {
    const ABSENT: u64 = 0x2;
    const LOCKED: u64 = 0x1;

    /// Returns `true` if the record was removed from the index.
    const fn is_absent(self) -> bool {
        self.0 & Self::ABSENT != 0
    }

    /// Returns `true` if the record is locked for writing.
    const fn is_locked(self) -> bool {
        self.0 & Self::LOCKED != 0
    }

    const fn with_absent(self) -> Self {
        Self(self.0 | Self::ABSENT)
    }

    const fn with_locked(self) -> Self {
        Self(self.0 | Self::LOCKED)
    }

    const fn without_locked(self) -> Self {
        Self(self.0 & !Self::LOCKED)
    }
}

trait IntoRawParts {
    fn into_raw_parts(self) -> (*mut u8, usize);
}

impl IntoRawParts for Option<Box<[u8]>> {
    fn into_raw_parts(self) -> (*mut u8, usize) {
        match self {
            Some(value) => {
                let len = value.len();
                let ptr = Box::into_raw(value).cast();
                (ptr, len)
            }
            None => (std::ptr::null_mut(), 0),
        }
    }
}
