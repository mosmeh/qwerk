use super::{ConcurrencyControl, ConcurrencyControlInternal, TransactionExecutor};
use crate::{
    lock::NoWaitRwLock,
    qsbr::{Qsbr, QsbrGuard},
    Error, Result, Shared,
};
use scc::{hash_index::Entry, HashIndex};
use std::cell::UnsafeCell;

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
            qsbr: self.qsbr.acquire(),
            garbage_records: Default::default(),
            working_set: Default::default(),
        }
    }
}

pub struct Record {
    value: UnsafeCell<Option<Box<[u8]>>>,
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
    // global state
    index: &'a HashIndex<Box<[u8]>, Shared<Record>>,

    // per-executor state
    qsbr: QsbrGuard<'a>,
    garbage_records: Vec<Shared<Record>>,

    // per-transaction state
    working_set: Vec<WorkingItem>,
}

impl TransactionExecutor for Executor<'_> {
    fn begin_transaction(&mut self) {
        self.working_set.clear();
    }

    fn read(&mut self, key: &[u8]) -> Result<Option<&[u8]>> {
        let item = self
            .working_set
            .iter()
            .find(|item| item.key.as_ref() == key);
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
                let item = WorkingItem {
                    key: key.to_vec().into(),
                    record_ptr,
                    kind: ItemKind::Read,
                };
                let value = unsafe { record.get() };
                (item, value)
            }
            Entry::Vacant(entry) => {
                let record_ptr = Shared::new(Record {
                    value: None.into(),
                    lock: NoWaitRwLock::new_locked_shared(),
                });
                entry.insert_entry(record_ptr);
                let item = WorkingItem {
                    key: key.to_vec().into(),
                    record_ptr,
                    kind: ItemKind::Read,
                };
                (item, None)
            }
        };
        self.working_set.push(item);
        Ok(value)
    }

    fn write(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        let item = self
            .working_set
            .iter_mut()
            .find(|item| item.key.as_ref() == key);
        if let Some(item) = item {
            let record = unsafe { item.record_ptr.as_ref() };
            match &item.kind {
                ItemKind::Read => {
                    if !record.lock.try_upgrade() {
                        return Err(Error::NotSerializable);
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
                WorkingItem {
                    key: key.to_vec().into(),
                    record_ptr,
                    kind: ItemKind::Write { original_value },
                }
            }
            Entry::Vacant(entry) => {
                let value = value.map(|value| value.to_vec().into());
                let record_ptr = Shared::new(Record {
                    value: value.into(),
                    lock: NoWaitRwLock::new_locked_exclusive(),
                });
                entry.insert_entry(record_ptr);
                WorkingItem {
                    key: key.to_vec().into(),
                    record_ptr,
                    kind: ItemKind::Write {
                        original_value: None,
                    },
                }
            }
        };
        self.working_set.push(item);
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        for item in &self.working_set {
            let record = unsafe { item.record_ptr.as_ref() };
            if unsafe { record.get() }.is_none() {
                self.index.remove(&item.key);
                self.garbage_records.push(item.record_ptr);
                continue;
            }
            match item.kind {
                ItemKind::Read => record.lock.unlock_shared(),
                ItemKind::Write { .. } => record.lock.unlock_exclusive(),
            }
        }
        self.finish_txn();
        Ok(())
    }

    fn abort(&mut self) {
        for item in self.working_set.drain(..) {
            let record = unsafe { item.record_ptr.as_ref() };
            match item.kind {
                ItemKind::Read if unsafe { record.get() }.is_none() => {
                    self.index.remove(&item.key);
                    self.garbage_records.push(item.record_ptr);
                }
                ItemKind::Read => record.lock.unlock_shared(),
                ItemKind::Write {
                    original_value: None,
                } => {
                    self.index.remove(&item.key);
                    self.garbage_records.push(item.record_ptr);
                }
                ItemKind::Write { original_value } => {
                    unsafe { record.set(original_value) };
                    record.lock.unlock_exclusive();
                }
            }
        }
        self.finish_txn();
    }
}

impl Executor<'_> {
    fn finish_txn(&mut self) {
        let garbage_bytes = self
            .garbage_records
            .len()
            .saturating_mul(std::mem::size_of::<Record>());
        if garbage_bytes >= super::GC_THRESHOLD_BYTES {
            self.collect_garbage();
        } else {
            self.qsbr.quiesce();
        }
    }

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

struct WorkingItem {
    key: Box<[u8]>,
    record_ptr: Shared<Record>,
    kind: ItemKind,
}

enum ItemKind {
    Read,
    Write { original_value: Option<Box<[u8]>> },
}
