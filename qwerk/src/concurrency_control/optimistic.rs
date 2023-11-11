use super::{ConcurrencyControl, ConcurrencyControlInternal, Shared, TransactionExecutor};
use crate::{
    epoch::{EpochFramework, EpochGuard},
    qsbr::{Qsbr, QsbrGuard},
    Error, Result,
};
use crossbeam_utils::Backoff;
use scc::{hash_index::Entry, HashIndex};
use std::{
    cell::OnceCell,
    sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering::SeqCst},
};

const EPOCH_POS: u64 = 32;
const SEQUENCE_POS: u64 = 1;
const LOCKED: u64 = 0x1;

// TID
// bits 63:32] - epoch
// bits[31:1]  - sequence
// bit [0]     - locked

/// Silo
pub struct Optimistic {
    epoch_fw: EpochFramework,
    qsbr: Qsbr,
}

impl ConcurrencyControl for Optimistic {}

impl ConcurrencyControlInternal for Optimistic {
    type Record = Record;
    type Executor<'a> = Executor<'a>;

    fn init() -> Self {
        Self {
            epoch_fw: Default::default(),
            qsbr: Default::default(),
        }
    }

    fn spawn_executor<'a>(
        &'a self,
        index: &'a HashIndex<Box<[u8]>, Shared<Self::Record>>,
    ) -> Self::Executor<'a> {
        Self::Executor {
            index,
            max_tid: 0,
            epoch_guard: self.epoch_fw.acquire(),
            qsbr: self.qsbr.acquire(),
            garbage_values: Default::default(),
            garbage_records: Default::default(),
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

impl Clone for Record {
    fn clone(&self) -> Self {
        let RecordSnapshot { buf_ptr, len, tid } = self.read();
        Self {
            buf_ptr: buf_ptr.into(),
            len: len.into(),
            tid: tid.into(),
        }
    }
}

impl Record {
    fn lock(&self) -> u64 {
        let backoff = Backoff::new();
        loop {
            let current_tid = self.tid.load(SeqCst);
            if current_tid & LOCKED > 0 {
                backoff.snooze();
                continue;
            }

            let result =
                self.tid
                    .compare_exchange_weak(current_tid, current_tid | LOCKED, SeqCst, SeqCst);
            if result.is_ok() {
                return current_tid;
            }
            backoff.spin();
        }
    }

    fn unlock(&self) {
        let prev_tid = self.tid.fetch_and(!LOCKED, SeqCst);
        debug_assert!(prev_tid & LOCKED > 0);
    }

    fn read(&self) -> RecordSnapshot {
        let backoff = Backoff::new();
        loop {
            let tid1 = self.tid.load(SeqCst);
            if tid1 & LOCKED > 0 {
                backoff.snooze();
                continue;
            }

            let buf_ptr = self.buf_ptr.load(SeqCst);
            let len = self.len.load(SeqCst);

            let tid2 = self.tid.load(SeqCst);
            if tid1 == tid2 {
                return RecordSnapshot {
                    buf_ptr,
                    len,
                    tid: tid2,
                };
            }
            backoff.spin();
        }
    }
}

struct RecordSnapshot {
    buf_ptr: *mut u8,
    len: usize,
    tid: u64,
}

pub struct Executor<'a> {
    // global state
    index: &'a HashIndex<Box<[u8]>, Shared<Record>>,

    // executor's state
    max_tid: u64,
    epoch_guard: EpochGuard<'a>,
    qsbr: QsbrGuard<'a>,
    garbage_values: Vec<Box<[u8]>>,
    garbage_records: Vec<Shared<Record>>,

    // transaction's state
    read_set: Vec<ReadItem<'a>>,
    write_set: Vec<WriteItem>,
}

impl TransactionExecutor for Executor<'_> {
    fn begin_transaction(&mut self) {
        self.read_set.clear();
        self.write_set.clear();
    }

    fn read(&mut self, key: &[u8]) -> Result<Option<&[u8]>> {
        let item = self.write_set.iter().find(|item| item.key.as_ref() == key);
        if let Some(item) = item {
            return Ok(item.value.as_deref());
        }

        let item = self.read_set.iter().find(|item| item.key.as_ref() == key);
        if let Some(item) = item {
            return Ok(item.value);
        }

        let record_ptr = self.index.peek_with(key, |_, value| *value);
        let key = key.to_vec().into();
        let item = match record_ptr {
            Some(record_ptr) => {
                let RecordSnapshot { buf_ptr, len, tid } = unsafe { record_ptr.as_ref() }.read();
                let value = if buf_ptr.is_null() {
                    None
                } else {
                    Some(unsafe { std::slice::from_raw_parts(buf_ptr, len) })
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
                tid: 0,
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
            item.tid |= LOCKED;
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
            key: key.to_vec().into(),
            value,
            record_ptr: Default::default(),
        });
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        // Phase 1

        // the stability of the sort doesn't matter because the keys are unique
        self.write_set.sort_unstable_by(|a, b| a.key.cmp(&b.key));

        // new TID should be
        // (b) larger than the worker’s most recently chosen TID
        let mut max_tid = self.max_tid;

        for item in &mut self.write_set {
            let mut was_occupied = false;
            let record_ptr = match self.index.entry(item.key.clone()) {
                Entry::Occupied(entry) => {
                    was_occupied = true;
                    *entry.get()
                }
                Entry::Vacant(entry) => {
                    let record_ptr = Shared::new(Record {
                        buf_ptr: AtomicPtr::new(std::ptr::null_mut()),
                        len: 0.into(),
                        tid: LOCKED.into(),
                    });
                    entry.insert_entry(record_ptr);
                    record_ptr
                }
            };
            if was_occupied {
                let tid = unsafe { record_ptr.as_ref() }.lock();

                // new TID should be
                // (a) larger than the TID of any record read or written
                //     by the transaction
                max_tid = max_tid.max(tid);
            }
            item.record_ptr
                .set(record_ptr)
                .unwrap_or_else(|_| panic!("record_ptr is already set"));
        }

        // serialization point
        let current_epoch = self.epoch_guard.refresh();

        // Phase 2
        for item in &self.read_set {
            let tid = item
                .record_ptr
                .or_else(|| self.index.peek_with(&item.key, |_, value| *value))
                .map(|record_ptr| unsafe { record_ptr.as_ref() }.tid.load(SeqCst))
                .unwrap_or(0);
            if tid != item.tid {
                // the TID doesn't match or the record is locked by
                // another transaction
                return Err(Error::NotSerializable);
            }

            // new TID should be
            // (a) larger than the TID of any record read or written
            //     by the transaction
            max_tid = max_tid.max(item.tid & !LOCKED);
        }

        // new TID should be
        // (c) in the current global epoch
        let epoch_of_max_tid = max_tid >> EPOCH_POS;
        debug_assert!(epoch_of_max_tid <= current_epoch);
        let mut new_tid = if epoch_of_max_tid == current_epoch {
            max_tid
        } else {
            current_epoch << EPOCH_POS
        };
        new_tid += 1 << SEQUENCE_POS;
        debug_assert!(new_tid & LOCKED == 0);
        self.max_tid = new_tid;

        // Phase 3
        for item in self.write_set.drain(..) {
            let record_ptr = *item.record_ptr.get().unwrap();
            let record = unsafe { record_ptr.as_ref() };

            let (new_buf_ptr, new_len) = match item.value {
                Some(value) => {
                    let len = value.len();
                    let ptr = Box::into_raw(value).cast();
                    (ptr, len)
                }
                None => (std::ptr::null_mut(), 0),
            };
            let prev_buf_ptr = record.buf_ptr.swap(new_buf_ptr, SeqCst);
            let prev_len = record.len.swap(new_len, SeqCst);

            if new_buf_ptr.is_null() {
                let was_removed = self.index.remove(&item.key);
                debug_assert!(was_removed);
            }

            // store new TID and unlock the record
            record.tid.store(new_tid, SeqCst);

            if !prev_buf_ptr.is_null() {
                self.garbage_values.push(unsafe {
                    Box::from_raw(std::slice::from_raw_parts_mut(prev_buf_ptr, prev_len))
                });
            }
            if new_buf_ptr.is_null() {
                self.garbage_records.push(record_ptr);
            }
        }

        self.finish_txn();
        Ok(())
    }

    fn abort(&mut self) {
        for item in &self.write_set {
            let Some(&record_ptr) = item.record_ptr.get() else {
                continue;
            };

            let record = unsafe { record_ptr.as_ref() };
            let value_is_null = record.buf_ptr.load(SeqCst).is_null();
            if value_is_null {
                let was_removed = self.index.remove(&item.key);
                debug_assert!(was_removed);
            }
            record.unlock();

            if value_is_null {
                self.garbage_records.push(record_ptr);
            }
        }

        self.finish_txn();
    }
}

impl Executor<'_> {
    fn finish_txn(&mut self) {
        if self.garbage_values.len() + self.garbage_records.len() >= 128 {
            self.collect_garbage();
        } else {
            self.qsbr.quiesce();
        }
    }

    fn collect_garbage(&mut self) {
        self.qsbr.sync();
        self.garbage_values.clear();
        for record_ptr in self.garbage_records.drain(..) {
            let _ = unsafe { Shared::into_box(record_ptr) };
        }
    }
}

impl Drop for Executor<'_> {
    fn drop(&mut self) {
        if !self.garbage_values.is_empty() || !self.garbage_records.is_empty() {
            self.collect_garbage();
        }
    }
}

struct ReadItem<'a> {
    key: Box<[u8]>,
    value: Option<&'a [u8]>,
    record_ptr: Option<Shared<Record>>,
    tid: u64,
}

struct WriteItem {
    key: Box<[u8]>,
    value: Option<Box<[u8]>>,
    record_ptr: OnceCell<Shared<Record>>,
}
