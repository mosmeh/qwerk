use super::{ConcurrencyControl, ConcurrencyControlInternal, TransactionExecutor};
use crate::{
    epoch::{Epoch, EpochGuard},
    lock::NoWaitRwLock,
    log::Logger,
    qsbr::{Qsbr, QsbrGuard},
    tid::{Tid, TidGenerator},
    Error, Result, Shared,
};
use scc::{hash_index::Entry, HashIndex};
use std::cell::{Cell, UnsafeCell};

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
        index: &'a HashIndex<Box<[u8]>, Shared<Self::Record>>,
    ) -> Self::Executor<'a> {
        Self::Executor {
            index,
            qsbr: &self.qsbr,
            tid_generator: Default::default(),
            qsbr_guard: self.qsbr.acquire(),
            garbage_records: Default::default(),
            rw_set: Default::default(),
        }
    }
}

pub struct Record {
    value: UnsafeCell<Option<Box<[u8]>>>,
    tid: Cell<Tid>,
    lock: NoWaitRwLock,
}

unsafe impl Sync for Record {}

impl Record {
    unsafe fn get(&self) -> Option<&[u8]> {
        assert!(self.lock.is_locked());
        (*self.value.get()).as_deref()
    }

    unsafe fn set(&self, value: Option<Box<[u8]>>) {
        assert!(self.lock.is_locked_exclusive());
        *self.value.get() = value;
    }

    unsafe fn replace(&self, value: Option<Box<[u8]>>) -> Option<Box<[u8]>> {
        assert!(self.lock.is_locked_exclusive());
        std::mem::replace(&mut *self.value.get(), value)
    }
}

pub struct Executor<'a> {
    // Global state
    index: &'a HashIndex<Box<[u8]>, Shared<Record>>,
    qsbr: &'a Qsbr,

    // Per-executor state
    tid_generator: TidGenerator,
    qsbr_guard: QsbrGuard<'a>,
    garbage_records: Vec<Shared<Record>>,

    // Per-transaction state
    rw_set: Vec<RwItem>,
}

impl TransactionExecutor for Executor<'_> {
    fn begin_transaction(&mut self) {
        self.rw_set.clear();
        self.qsbr_guard.quiesce();
    }

    fn end_transaction(&mut self) {
        self.qsbr_guard.mark_as_offline();

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

        let (item, value) = match self.index.entry(key.to_vec().into()) {
            Entry::Occupied(entry) => {
                let record_ptr = *entry.get();
                let record = unsafe { record_ptr.as_ref() };
                if !record.lock.try_lock_shared() {
                    return Err(Error::NotSerializable);
                }
                let item = RwItem {
                    key: key.to_vec().into(),
                    record_ptr,
                    kind: ItemKind::Read { was_occupied: true },
                };
                let value = unsafe { record.get() };
                (item, value)
            }
            Entry::Vacant(entry) => {
                let record_ptr = Shared::new(Record {
                    value: None.into(),
                    tid: Default::default(),
                    lock: NoWaitRwLock::new_locked_exclusive(),
                });
                entry.insert_entry(record_ptr);
                let item = RwItem {
                    key: key.to_vec().into(),
                    record_ptr,
                    kind: ItemKind::Read {
                        was_occupied: false,
                    },
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
                ItemKind::Read { was_occupied } => {
                    if *was_occupied {
                        if !record.lock.try_upgrade() {
                            return Err(Error::NotSerializable);
                        }
                    } else {
                        assert!(record.lock.is_locked_exclusive());
                    }
                    let value = value.map(|value| value.to_vec().into());
                    let original_value = unsafe { record.replace(value) };
                    item.kind = ItemKind::Write { original_value };
                }
                ItemKind::Write { .. } => {
                    let value = value.map(|value| value.to_vec().into());
                    unsafe { record.set(value) };
                }
            }
            return Ok(());
        }

        let item = match self.index.entry(key.to_vec().into()) {
            Entry::Occupied(entry) => {
                let record_ptr = *entry.get();
                let record = unsafe { record_ptr.as_ref() };
                if !record.lock.try_lock_exclusive() {
                    return Err(Error::NotSerializable);
                }
                let value = value.map(|value| value.to_vec().into());
                let original_value = unsafe { record.replace(value) };
                RwItem {
                    key: key.to_vec().into(),
                    record_ptr,
                    kind: ItemKind::Write { original_value },
                }
            }
            Entry::Vacant(entry) => {
                let value = value.map(|value| value.to_vec().into());
                let record_ptr = Shared::new(Record {
                    value: value.into(),
                    tid: Default::default(),
                    lock: NoWaitRwLock::new_locked_exclusive(),
                });
                entry.insert_entry(record_ptr);
                RwItem {
                    key: key.to_vec().into(),
                    record_ptr,
                    kind: ItemKind::Write {
                        original_value: None,
                    },
                }
            }
        };
        self.rw_set.push(item);
        Ok(())
    }

    fn commit(&mut self, epoch_guard: &EpochGuard, logger: &Logger) -> Result<Epoch> {
        let mut tid_rw_set = self.tid_generator.begin_transaction();
        let mut reserver = logger.reserver();
        for item in &self.rw_set {
            let record = unsafe { item.record_ptr.as_ref() };
            tid_rw_set.add(record.tid.get());
            if let ItemKind::Write { .. } = item.kind {
                reserver.reserve_write(&item.key, unsafe { record.get() });
            }
        }
        let reserved = reserver.finish();

        let epoch = epoch_guard.refresh();
        let new_tid = tid_rw_set.generate_tid(epoch);

        let mut entry = reserved.insert(new_tid);
        for item in &self.rw_set {
            let record = unsafe { item.record_ptr.as_ref() };
            let value = unsafe { record.get() };
            if let ItemKind::Write { .. } = &item.kind {
                entry.write(&item.key, value);
            }
            if value.is_none() {
                // The record is removed while being exclusively locked.
                // This makes sure other transactions concurrently accessing
                // the record abort, as we are using NO_WAIT deadlock
                // prevention.
                // This also applies to the record removal in abort().
                assert!(record.lock.is_locked_exclusive());
                self.index.remove(&item.key);
                self.garbage_records.push(item.record_ptr);
                continue;
            }
            match item.kind {
                ItemKind::Read { was_occupied } => {
                    assert!(was_occupied);
                    record.lock.unlock_shared();
                }
                ItemKind::Write { .. } => {
                    record.tid.set(new_tid);
                    record.lock.unlock_exclusive();
                }
            }
        }
        Ok(epoch)
    }

    fn abort(&mut self) {
        for item in self.rw_set.drain(..) {
            let record = unsafe { item.record_ptr.as_ref() };
            match item.kind {
                ItemKind::Read {
                    was_occupied: false,
                } => {
                    assert!(record.lock.is_locked_exclusive());
                    assert!(unsafe { record.get() }.is_none());
                    self.index.remove(&item.key);
                    self.garbage_records.push(item.record_ptr);
                }
                ItemKind::Read { was_occupied: true } => record.lock.unlock_shared(),
                ItemKind::Write {
                    original_value: None,
                } => {
                    assert!(record.lock.is_locked_exclusive());
                    self.index.remove(&item.key);
                    self.garbage_records.push(item.record_ptr);
                }
                ItemKind::Write { original_value } => {
                    unsafe { record.set(original_value) };
                    record.lock.unlock_exclusive();
                }
            }
        }
    }
}

impl Executor<'_> {
    fn collect_garbage(&mut self) {
        self.qsbr.sync();
        for record_ptr in self.garbage_records.drain(..) {
            let _ = unsafe { Shared::into_box(record_ptr) };
        }
    }
}

impl Drop for Executor<'_> {
    fn drop(&mut self) {
        if !self.garbage_records.is_empty() {
            self.collect_garbage();
        }
    }
}

/// An item in the read or write set.
struct RwItem {
    key: Box<[u8]>,
    record_ptr: Shared<Record>,
    kind: ItemKind,
}

enum ItemKind {
    Read { was_occupied: bool },
    Write { original_value: Option<Box<[u8]>> },
}
