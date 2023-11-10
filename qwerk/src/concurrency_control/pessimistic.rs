use super::{ConcurrencyControl, RecordPtr, TransactionExecutor};
use crate::{
    lock::NoWaitRwLock,
    qsbr::{Qsbr, QsbrGuard},
    Error, Result,
};
use scc::{hash_index::Entry, HashIndex};
use std::{cell::UnsafeCell, ptr::NonNull};

/// Strong strict two phase locking with NO_WAIT deadlock prevention
#[derive(Default)]
pub struct Pessimistic {
    qsbr: Qsbr,
}

impl ConcurrencyControl for Pessimistic {
    type Record = Record;
    type Executor<'a> = Executor<'a>;

    fn spawn_executor<'a>(
        &'a self,
        index: &'a HashIndex<Box<[u8]>, RecordPtr<Self::Record>>,
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
        debug_assert!(self.lock.is_locked());
        (*self.value.get()).as_deref()
    }

    unsafe fn set(&self, value: Option<Box<[u8]>>) {
        debug_assert!(self.lock.is_locked());
        *self.value.get() = value;
    }

    unsafe fn replace(&self, value: Option<Box<[u8]>>) -> Option<Box<[u8]>> {
        debug_assert!(self.lock.is_locked());
        std::mem::replace(&mut *self.value.get(), value)
    }
}

pub struct Executor<'a> {
    index: &'a HashIndex<Box<[u8]>, RecordPtr<Record>>,
    qsbr: QsbrGuard<'a>,
    garbage_records: Vec<NonNull<Record>>,
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
                let record_ptr = entry.get().0;
                let record = unsafe { &mut *record_ptr.as_ptr() };
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
                let record_ptr = NonNull::from(Box::leak(Box::new(Record {
                    value: None.into(),
                    lock: NoWaitRwLock::new_locked_shared(),
                })));
                entry.insert_entry(record_ptr.into());
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
            if matches!(item.kind, ItemKind::Read) {
                if !record.lock.try_upgrade() {
                    return Err(Error::NotSerializable);
                }
                let value = value.map(|value| value.to_vec().into());
                let original_value = unsafe { record.replace(value) };
                item.kind = ItemKind::Write { original_value };
                return Ok(());
            }
            let value = value.map(|value| value.to_vec().into());
            unsafe { record.set(value) };
            return Ok(());
        }

        let item = match self.index.entry(key.to_vec().into()) {
            Entry::Occupied(entry) => {
                let record_ptr = entry.get().0;
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
                let record_ptr = NonNull::from(Box::leak(Box::new(Record {
                    value: value.into(),
                    lock: NoWaitRwLock::new_locked_exclusive(),
                })));
                entry.insert_entry(record_ptr.into());
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
        if self.garbage_records.len() >= 128 {
            self.collect_garbage();
        } else {
            self.qsbr.quiesce();
        }
    }

    fn collect_garbage(&mut self) {
        self.qsbr.sync();
        for record_ptr in self.garbage_records.drain(..) {
            let _ = unsafe { Box::from_raw(record_ptr.as_ptr()) };
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
    record_ptr: NonNull<Record>,
    kind: ItemKind,
}

enum ItemKind {
    Read,
    Write { original_value: Option<Box<[u8]>> },
}
