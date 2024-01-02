use super::{ConcurrencyControl, ConcurrencyControlInternal, TransactionExecutor};
use crate::{
    epoch::EpochParticipant,
    lock::Lock,
    memory_reclamation::Reclaimer,
    persistence::LogEntry,
    record,
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
#[allow(clippy::doc_markdown)]
#[derive(Default)]
pub struct Pessimistic {
    _private: (), // Ensures forward compatibility when a field is added.
}

impl Pessimistic {
    pub fn new() -> Self {
        Default::default()
    }
}

impl ConcurrencyControl for Pessimistic {}

impl ConcurrencyControlInternal for Pessimistic {
    type Record = Record;
    type Executor<'a> = Executor<'a>;

    fn load_record(
        index: &Index<Self::Record>,
        key: SmallBytes,
        value: Option<Box<[u8]>>,
        tid: Tid,
    ) {
        match index.entry(key) {
            Entry::Occupied(entry) => {
                let record_ptr = *entry.get();
                let record = unsafe { record_ptr.as_ref() };
                let _guard = record.lock.write().unwrap();
                if tid > record.tid.get() {
                    let value = value.map(Into::into);
                    unsafe { record.set(value) };
                    record.tid.set(tid);
                }
            }
            Entry::Vacant(entry) => {
                let record_ptr = Shared::new(Record {
                    value: value.map(Into::into).into(),
                    tid: tid.into(),
                    lock: Lock::new_unlocked(),
                });
                entry.insert_entry(record_ptr);
            }
        }
    }

    fn executor<'a>(
        &'a self,
        index: &'a Index<Self::Record>,
        epoch_participant: EpochParticipant<'a>,
        reclaimer: Reclaimer<'a, Self::Record>,
    ) -> Self::Executor<'a> {
        Self::Executor {
            index,
            epoch_participant,
            reclaimer,
            tid_generator: Default::default(),
            removal_queue: Default::default(),
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

impl record::Record for Record {
    fn peek<F, T>(&self, f: F) -> Option<T>
    where
        F: FnOnce(&[u8], Tid) -> T,
    {
        self.lock
            .read()
            .and_then(|_guard| unsafe { self.get() }.map(|value| f(value, self.tid.get())))
    }

    fn is_tombstone(&self) -> bool {
        self.lock
            .read()
            .map_or(true, |_guard| unsafe { self.get() }.is_none())
    }
}

pub struct Executor<'a> {
    // Global state
    index: &'a Index<Record>,

    // Per-executor state
    epoch_participant: EpochParticipant<'a>,
    reclaimer: Reclaimer<'a, Record>,
    tid_generator: TidGenerator,
    removal_queue: VecDeque<(SmallBytes, Tid)>,

    // Per-transaction state
    rw_set: Vec<RwItem>,
}

impl TransactionExecutor for Executor<'_> {
    fn begin_transaction(&mut self) {
        self.rw_set.clear();
        std::mem::forget(self.reclaimer.enter());
        std::mem::forget(self.epoch_participant.acquire());
    }

    fn end_transaction(&mut self) {
        self.epoch_participant.force_release();
        self.reclaimer.force_leave();
        self.process_removal_queue();
        self.reclaimer.reclaim();
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
                    return Err(Error::TransactionNotSerializable);
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
                            return Err(Error::TransactionNotSerializable);
                        };
                        std::mem::forget(guard);
                    }
                    let value = value.map(Into::into);
                    let original_value = unsafe { record.replace(value) };
                    item.kind = ItemKind::Write { original_value };
                }
                ItemKind::Write { .. } => {
                    let value = value.map(Into::into);
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
                    return Err(Error::TransactionNotSerializable);
                };
                std::mem::forget(guard);
                let value = value.map(Into::into);
                let original_value = unsafe { record.replace(value) };
                RwItem {
                    key: key.into(),
                    record_ptr,
                    was_vacant: false,
                    kind: ItemKind::Write { original_value },
                }
            }
            Entry::Vacant(entry) => {
                let value = value.map(Into::into);
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

    fn validate(&mut self) -> Result<Tid> {
        // Validation always succeeds because the records are already locked.
        // Just generate a new TID.
        let mut tid_set = self.tid_generator.transaction();
        for item in &self.rw_set {
            let record = unsafe { item.record_ptr.as_ref() };
            tid_set.add(record.tid.get());
        }
        let commit_epoch = self.epoch_participant.force_refresh();
        tid_set
            .generate_tid(commit_epoch)
            .ok_or(Error::TooManyTransactions)
    }

    fn log(&self, log_entry: &mut LogEntry<'_>) {
        for item in &self.rw_set {
            if let ItemKind::Write { .. } = &item.kind {
                let record = unsafe { item.record_ptr.as_ref() };
                let value = unsafe { record.get() };
                log_entry.write(&item.key, value);
            }
        }
    }

    fn precommit(&mut self, commit_tid: Tid) {
        for item in self.rw_set.drain(..) {
            let record = unsafe { item.record_ptr.as_ref() };
            let is_tombstone = unsafe { record.get() }.is_none();
            match item.kind {
                ItemKind::Read if item.was_vacant => {
                    record.lock.kill();
                    self.index.remove(&item.key);
                    self.reclaimer
                        .defer_drop(unsafe { item.record_ptr.into_box() });
                }
                ItemKind::Read => record.lock.force_unlock_read(),
                ItemKind::Write { .. } => {
                    record.tid.set(commit_tid);
                    record.lock.force_unlock_write();
                    if is_tombstone {
                        self.removal_queue.push_back((item.key, commit_tid));
                    }
                }
            }
        }
    }

    fn abort(&mut self) {
        for item in self.rw_set.drain(..) {
            let record = unsafe { item.record_ptr.as_ref() };
            if item.was_vacant {
                record.lock.kill();
                self.index.remove(&item.key);
                self.reclaimer
                    .defer_drop(unsafe { item.record_ptr.into_box() });
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
    fn process_removal_queue(&mut self) {
        let reclamation_epoch = self.epoch_participant.reclamation_epoch();
        while let Some((_, tid)) = self.removal_queue.front() {
            if tid.epoch() > reclamation_epoch {
                break;
            }
            let (key, tid) = self.removal_queue.pop_front().unwrap();
            let mut enter_guard = self.reclaimer.enter();
            self.index.remove_if(&key, |record_ptr| {
                let record = unsafe { record_ptr.as_ref() };
                let Some(write_guard) = record.lock.write() else {
                    return true;
                };
                if record.tid.get() != tid {
                    return false;
                }
                assert!(unsafe { record.get() }.is_none());
                write_guard.kill();
                enter_guard.defer_drop(unsafe { record_ptr.into_box() });
                true
            });
        }
    }
}

impl Drop for Executor<'_> {
    fn drop(&mut self) {
        let backoff = Backoff::new();
        loop {
            self.process_removal_queue();
            if self.removal_queue.is_empty() {
                break;
            }
            backoff.snooze();
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
