use super::{ConcurrencyControl, ConcurrencyControlInternal, TransactionExecutor};
use crate::{
    epoch::{Epoch, EpochGuard},
    lock::Lock,
    log::Logger,
    qsbr::{Qsbr, QsbrGuard},
    small_bytes::SmallBytes,
    tid::{Tid, TidGenerator},
    Error, Index, Result, Shared,
};
use crossbeam_utils::Backoff;
use scc::hash_index::Entry;
use std::{
    cell::{Cell, UnsafeCell},
    collections::VecDeque,
};

/// Pessimistic concurrency control.
///
/// This is an implementation of strong strict two phase locking with
/// NO_WAIT deadlock prevention.
pub struct Pessimistic {
    qsbr: Qsbr,
}

impl ConcurrencyControl for Pessimistic {}

impl ConcurrencyControlInternal for Pessimistic {
    type Record = Record;
    type Executor<'a> = Executor<'a>;

    fn init() -> Self {
        Self {
            qsbr: Default::default(),
        }
    }

    fn spawn_executor<'a>(
        &'a self,
        index: &'a Index<Self::Record>,
        epoch_guard: EpochGuard<'a>,
    ) -> Self::Executor<'a> {
        Self::Executor {
            index,
            qsbr: &self.qsbr,
            epoch_guard,
            tid_generator: Default::default(),
            qsbr_guard: self.qsbr.acquire(),
            remove_queue: Default::default(),
            garbage_records: Default::default(),
            rw_set: Default::default(),
        }
    }
}

pub struct Record {
    value: UnsafeCell<Option<SmallBytes>>,
    tid: Cell<Tid>,
    lock: Lock,
}

unsafe impl Sync for Record {}

impl Record {
    unsafe fn get(&self) -> Option<&[u8]> {
        assert!(self.lock.is_locked());
        (*self.value.get()).as_deref()
    }

    unsafe fn set(&self, value: Option<SmallBytes>) {
        assert!(self.lock.is_locked_exclusive());
        *self.value.get() = value;
    }

    unsafe fn replace(&self, value: Option<SmallBytes>) -> Option<SmallBytes> {
        assert!(self.lock.is_locked_exclusive());
        std::mem::replace(&mut *self.value.get(), value)
    }
}

pub struct Executor<'a> {
    // Global state
    index: &'a Index<Record>,
    qsbr: &'a Qsbr,

    // Per-executor state
    epoch_guard: EpochGuard<'a>,
    tid_generator: TidGenerator,
    qsbr_guard: QsbrGuard<'a>,
    remove_queue: VecDeque<(SmallBytes, Tid)>,
    garbage_records: Vec<Shared<Record>>,

    // Per-transaction state
    rw_set: Vec<RwItem>,
}

impl TransactionExecutor for Executor<'_> {
    fn begin_transaction(&mut self) {
        self.rw_set.clear();
        self.qsbr_guard.quiesce();
        self.epoch_guard.refresh();
    }

    fn end_transaction(&mut self) {
        self.epoch_guard.release();
        self.qsbr_guard.mark_as_offline();
        self.process_remove_queue();
        let garbage_bytes = self
            .garbage_records
            .len()
            .saturating_mul(std::mem::size_of::<Record>());
        if garbage_bytes >= super::GC_THRESHOLD_BYTES {
            self.collect_garbage();
        }
    }

    fn read(&mut self, key: &[u8]) -> Result<Option<&[u8]>> {
        let item = self.rw_set.iter().find(|item| item.key.as_ref() == key);
        if let Some(item) = item {
            return Ok(unsafe { item.record_ptr.as_ref().get() });
        }

        let (item, value) = match self.index.entry(key.into()) {
            Entry::Occupied(entry) => {
                let record_ptr = *entry.get();
                let record = unsafe { record_ptr.as_ref() };
                let Some(guard) = record.lock.try_read() else {
                    return Err(Error::NotSerializable);
                };
                std::mem::forget(guard);
                let item = RwItem {
                    key: key.into(),
                    record_ptr,
                    was_vacant: false,
                    kind: ItemKind::Read,
                };
                let value = unsafe { record.get() };
                (item, value)
            }
            Entry::Vacant(entry) => {
                let record_ptr = Shared::new(Record {
                    value: None.into(),
                    tid: Tid::ZERO.into(),
                    lock: Lock::new_locked_exclusive(),
                });
                entry.insert_entry(record_ptr);
                let item = RwItem {
                    key: key.into(),
                    record_ptr,
                    was_vacant: true,
                    kind: ItemKind::Read,
                };
                (item, None)
            }
        };
        self.rw_set.push(item);
        Ok(value)
    }

    fn write(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        let item = self.rw_set.iter_mut().find(|item| item.key.as_ref() == key);
        if let Some(item) = item {
            let record = unsafe { item.record_ptr.as_ref() };
            match &item.kind {
                ItemKind::Read => {
                    if item.was_vacant {
                        // We locked the record exclusively when we created it.
                        assert!(record.lock.is_locked_exclusive());
                    } else {
                        let Some(guard) = record.lock.try_upgrade() else {
                            return Err(Error::NotSerializable);
                        };
                        std::mem::forget(guard);
                    }
                    let value = value.map(|value| value.into());
                    let original_value = unsafe { record.replace(value) };
                    item.kind = ItemKind::Write { original_value };
                }
                ItemKind::Write { .. } => {
                    let value = value.map(|value| value.into());
                    unsafe { record.set(value) };
                }
            }
            return Ok(());
        }

        let item = match self.index.entry(key.into()) {
            Entry::Occupied(entry) => {
                let record_ptr = *entry.get();
                let record = unsafe { record_ptr.as_ref() };
                let Some(guard) = record.lock.try_write() else {
                    return Err(Error::NotSerializable);
                };
                std::mem::forget(guard);
                let value = value.map(|value| value.into());
                let original_value = unsafe { record.replace(value) };
                RwItem {
                    key: key.into(),
                    record_ptr,
                    was_vacant: false,
                    kind: ItemKind::Write { original_value },
                }
            }
            Entry::Vacant(entry) => {
                let value = value.map(|value| value.into());
                let record_ptr = Shared::new(Record {
                    value: value.into(),
                    tid: Tid::ZERO.into(),
                    lock: Lock::new_locked_exclusive(),
                });
                entry.insert_entry(record_ptr);
                RwItem {
                    key: key.into(),
                    record_ptr,
                    was_vacant: true,
                    kind: ItemKind::Write {
                        original_value: None,
                    },
                }
            }
        };
        self.rw_set.push(item);
        Ok(())
    }

    fn commit(&mut self, logger: &Logger) -> Result<Epoch> {
        let mut tid_set = self.tid_generator.begin_transaction();
        let mut log_capacity_reserver = logger.reserver();
        for item in &self.rw_set {
            let record = unsafe { item.record_ptr.as_ref() };
            tid_set.add(record.tid.get());
            if let ItemKind::Write { .. } = item.kind {
                log_capacity_reserver.reserve_write(&item.key, unsafe { record.get() });
            }
        }
        let reserved_log_capacity = log_capacity_reserver.finish();

        let epoch = self.epoch_guard.refresh();
        let commit_tid = tid_set
            .generate_tid(epoch)
            .ok_or(Error::TooManyTransactions)?;

        let mut log_entry = reserved_log_capacity.insert(commit_tid);
        for item in &self.rw_set {
            let record = unsafe { item.record_ptr.as_ref() };
            let value = unsafe { record.get() };
            if let ItemKind::Write { .. } = &item.kind {
                log_entry.write(&item.key, value);
            }
            match item.kind {
                ItemKind::Read if item.was_vacant => record.lock.force_unlock_write(),
                ItemKind::Read => record.lock.force_unlock_read(),
                ItemKind::Write { .. } => {
                    record.tid.set(commit_tid);
                    let should_remove = value.is_none();
                    record.lock.force_unlock_write();
                    if should_remove {
                        self.remove_queue.push_back((item.key.clone(), commit_tid));
                    }
                }
            }
        }
        Ok(epoch)
    }

    fn abort(&mut self) {
        for item in self.rw_set.drain(..) {
            let record = unsafe { item.record_ptr.as_ref() };
            if item.was_vacant {
                // The record is removed while being exclusively locked.
                // This makes sure other transactions concurrently accessing
                // the record abort, as we are using NO_WAIT deadlock
                // prevention.
                // This also applies to the record removal in
                // process_remove_queue().
                assert!(record.lock.is_locked_exclusive());
                self.index.remove(&item.key);
                self.garbage_records.push(item.record_ptr);
                continue;
            }
            match item.kind {
                ItemKind::Read => record.lock.force_unlock_read(),
                ItemKind::Write { original_value } => {
                    unsafe { record.set(original_value) };
                    record.lock.force_unlock_write();
                }
            }
        }
    }
}

impl Executor<'_> {
    fn process_remove_queue(&mut self) {
        let reclamation_epoch = self.epoch_guard.reclamation_epoch();
        while let Some((_, tid)) = self.remove_queue.front() {
            if tid.epoch() > reclamation_epoch {
                break;
            }
            let (key, tid) = self.remove_queue.pop_front().unwrap();
            self.index.remove_if(&key, |record_ptr| {
                let record = unsafe { record_ptr.as_ref() };
                let guard = record.lock.write();
                if record.tid.get() != tid {
                    return false;
                }
                std::mem::forget(guard);
                self.garbage_records.push(*record_ptr);
                true
            });
        }
    }

    fn collect_garbage(&mut self) {
        self.qsbr.sync();
        for record_ptr in self.garbage_records.drain(..) {
            unsafe { record_ptr.drop_in_place() };
        }
    }
}

impl Drop for Executor<'_> {
    fn drop(&mut self) {
        let backoff = Backoff::new();
        while !self.remove_queue.is_empty() {
            self.process_remove_queue();
            backoff.snooze();
        }
        if !self.garbage_records.is_empty() {
            self.collect_garbage();
        }
    }
}

/// An item in the read or write set.
struct RwItem {
    key: SmallBytes,
    record_ptr: Shared<Record>,
    was_vacant: bool,
    kind: ItemKind,
}

enum ItemKind {
    Read,
    Write { original_value: Option<SmallBytes> },
}
