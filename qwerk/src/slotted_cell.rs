use parking_lot::Once;
use std::{
    cell::UnsafeCell,
    marker::PhantomData,
    mem::MaybeUninit,
    ops::Deref,
    sync::atomic::{AtomicBool, AtomicPtr, Ordering::SeqCst},
};

// Based on the design of thread_local crate
// https://github.com/Amanieu/thread_local-rs/blob/faa4409fafa3a5b4898c4e5025733f760a3eb665/src/lib.rs

const NUM_BUCKETS: usize = (usize::BITS - 1) as usize;

/// A cell that can store multiple values in locations called slots.
pub struct SlottedCell<T> {
    buckets: [AtomicPtr<Entry<T>>; NUM_BUCKETS],
}

impl<T> SlottedCell<T> {
    pub const fn iter(&self) -> Iter<T> {
        Iter::new(self)
    }
}

impl<T> SlottedCell<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        let num_allocated_buckets = (usize::BITS - capacity.leading_zeros()) as usize;

        let mut buckets = [std::ptr::null_mut(); NUM_BUCKETS];
        for (i, bucket) in buckets[..num_allocated_buckets].iter_mut().enumerate() {
            *bucket = alloc_bucket::<T>(bucket_len(i));
        }

        Self {
            // SAFETY: AtomicPtr has the same representation as a pointer and
            //         arrays have the same representation as
            //         a sequence of their inner type.
            buckets: unsafe { std::mem::transmute(buckets) },
        }
    }

    /// Returns the value at the given index, or `None` if the index is out of
    /// bounds or the slot is empty.
    pub fn get(&self, index: usize) -> Option<&T> {
        let bucket_index = (usize::BITS - (index + 1).leading_zeros() - 1) as usize;
        let entries = self.buckets[bucket_index].load(SeqCst);
        if entries.is_null() {
            return None;
        }
        let entry_index = index - (bucket_len(bucket_index) - 1);
        let entry = unsafe { &*entries.add(entry_index) };
        entry.get()
    }

    /// Allocates a slot in the cell, initializing it with a default value if
    /// the slot was empty.
    pub fn alloc(&self) -> Slot<T>
    where
        T: Default,
    {
        self.alloc_with(|_| Default::default())
    }

    /// Allocates a slot in the cell, initializing it with `f` if the slot was
    /// empty.
    pub fn alloc_with<F>(&self, f: F) -> Slot<T>
    where
        F: FnOnce(usize) -> T,
    {
        let mut index = 0;
        for (bucket_index, bucket) in self.buckets.iter().enumerate() {
            let bucket_len = bucket_len(bucket_index);
            let bucket_ptr = bucket.load(SeqCst);
            let entries = if bucket_ptr.is_null() {
                let new = alloc_bucket(bucket_len);
                let result = bucket.compare_exchange(std::ptr::null_mut(), new, SeqCst, SeqCst);
                match result {
                    Ok(_) => new,
                    Err(ptr) => {
                        unsafe { dealloc_bucket(new, bucket_len) };
                        ptr
                    }
                }
            } else {
                bucket_ptr
            };

            for entry_index in 0..bucket_len {
                let entry = unsafe { &*entries.add(entry_index) };
                let result = entry
                    .is_occupied
                    .compare_exchange(false, true, SeqCst, SeqCst);
                if result.is_ok() {
                    entry.init(index, f);
                    return Slot { entry };
                }
                index += 1;
            }
        }

        unreachable!("too many slots")
    }
}

impl<T> Default for SlottedCell<T> {
    fn default() -> Self {
        Self::with_capacity(2)
    }
}

impl<T> Drop for SlottedCell<T> {
    fn drop(&mut self) {
        for (i, bucket) in self.buckets.iter_mut().enumerate() {
            let ptr = *bucket.get_mut();
            if ptr.is_null() {
                continue;
            }
            let len = bucket_len(i);
            unsafe { dealloc_bucket(ptr, len) };
        }
    }
}

struct Entry<T> {
    is_occupied: AtomicBool,
    once: Once,
    value: UnsafeCell<MaybeUninit<T>>,
    phantom: PhantomData<T>,
}

impl<T> Default for Entry<T> {
    fn default() -> Self {
        Self {
            is_occupied: Default::default(),
            once: Once::new(),
            value: UnsafeCell::new(MaybeUninit::uninit()),
            phantom: Default::default(),
        }
    }
}

impl<T> Drop for Entry<T> {
    fn drop(&mut self) {
        if self.once.state().done() {
            unsafe { (*self.value.get()).assume_init_drop() };
        }
    }
}

impl<T> Entry<T> {
    fn get(&self) -> Option<&T> {
        self.once
            .state()
            .done()
            .then(|| unsafe { self.get_unchecked() })
    }

    unsafe fn get_unchecked(&self) -> &T {
        (*self.value.get()).assume_init_ref()
    }

    fn init<F>(&self, index: usize, f: F)
    where
        F: FnOnce(usize) -> T,
    {
        self.once.call_once_force(|_| {
            let value = f(index);
            unsafe { (*self.value.get()).write(value) };
        });
    }
}

fn alloc_bucket<T>(len: usize) -> *mut Entry<T> {
    let entries = (0..len).map(|_| Entry::<T>::default()).collect();
    Box::into_raw(entries).cast()
}

unsafe fn dealloc_bucket<T>(ptr: *mut Entry<T>, len: usize) {
    let _ = Box::from_raw(std::slice::from_raw_parts_mut(ptr, len));
}

const fn bucket_len(bucket_index: usize) -> usize {
    1 << bucket_index
}

/// A slot in a [`SlottedCell`].
///
/// Dropping a `Slot` will mark a slot as free and allow it to be
/// reused by a subsequent call to [`SlottedCell::alloc`] or
/// [`SlottedCell::alloc_with`].
/// However, the value will not be dropped and will remain in the slot until
/// the [`SlottedCell`] is dropped.
pub struct Slot<'a, T> {
    entry: &'a Entry<T>,
}

impl<T> Drop for Slot<'_, T> {
    fn drop(&mut self) {
        let was_occupied = self.entry.is_occupied.swap(false, SeqCst);
        assert!(was_occupied);
    }
}

impl<T> Deref for Slot<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { self.entry.get_unchecked() }
    }
}

/// An iterator over the values stored in slots of a [`SlottedCell`].
///
/// Note that while you are holding a reference to a slot, the slot may be
/// unassigned from [`Slot`] and assigned to another [`Slot`].
pub struct Iter<'a, T> {
    cell: &'a SlottedCell<T>,
    bucket_index: usize,
    entry_index: usize,
}

impl<'a, T> Iter<'a, T> {
    const fn new(cell: &'a SlottedCell<T>) -> Self {
        Self {
            cell,
            bucket_index: 0,
            entry_index: 0,
        }
    }
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        while self.bucket_index < NUM_BUCKETS {
            let bucket = unsafe { self.cell.buckets.get_unchecked(self.bucket_index) };
            let bucket_ptr = bucket.load(SeqCst);
            if bucket_ptr.is_null() {
                return None;
            }

            let bucket_len = bucket_len(self.bucket_index);
            while self.entry_index < bucket_len {
                let entry = unsafe { &*bucket_ptr.add(self.entry_index) };
                self.entry_index += 1;
                if let Some(value) = entry.get() {
                    return Some(value);
                }
            }

            self.bucket_index += 1;
            self.entry_index = 0;
        }
        None
    }
}
