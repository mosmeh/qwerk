use std::{
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

impl<T: Default> SlottedCell<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        let num_allocated_buckets = (usize::BITS - capacity.leading_zeros()) as usize;

        let mut buckets = [std::ptr::null_mut(); NUM_BUCKETS];
        for (i, bucket) in buckets[..num_allocated_buckets].iter_mut().enumerate() {
            *bucket = alloc_bucket::<T>(bucket_len(i));
        }

        Self {
            // SAFETY: AtomicPtr has the same representation as a pointer and arrays have the same
            // representation as a sequence of their inner type.
            buckets: unsafe { std::mem::transmute(buckets) },
        }
    }

    /// Allocates a slot in the cell.
    ///
    /// This may return an existing slot that was previously occupied by another
    /// [`Slot`]. Thus the initial value of the slot may not be
    /// the default value of `T`.
    pub fn alloc_slot(&self) -> Slot<T> {
        for (bucket_index, bucket) in self.buckets.iter().enumerate() {
            let bucket_len = bucket_len(bucket_index);
            let bucket_ptr = bucket.load(SeqCst);
            let entries = if bucket_ptr.is_null() {
                let new = alloc_bucket(bucket_len);
                let result = bucket.compare_exchange(std::ptr::null_mut(), new, SeqCst, SeqCst);
                match result {
                    Ok(_) => new,
                    Err(ptr) => ptr,
                }
            } else {
                bucket_ptr
            };

            for entry_index in 0..bucket_len {
                let entry = unsafe { &*entries.add(entry_index) };
                if !entry.is_present.fetch_or(true, SeqCst) {
                    return Slot { entry };
                }
            }
        }

        panic!("too many slots")
    }
}

impl<T: Default> Default for SlottedCell<T> {
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
            let _ = unsafe { Box::from_raw(std::slice::from_raw_parts_mut(ptr, len)) };
        }
    }
}

#[derive(Default)]
struct Entry<T> {
    is_present: AtomicBool,
    value: T,
}

fn alloc_bucket<T: Default>(len: usize) -> *mut Entry<T> {
    let entries = (0..len).map(|_| Entry::<T>::default()).collect();
    Box::into_raw(entries).cast()
}

const fn bucket_len(index: usize) -> usize {
    1 << index
}

/// A slot in a [`SlottedCell`].
///
/// Dropping a `Slot` will mark a slot as free and allow it to be
/// reused by a subsequent call to [`SlottedCell::alloc_slot`].
/// However, the value will not be dropped and will remain in memory until
/// the [`SlottedCell`] is dropped.
pub struct Slot<'a, T> {
    entry: &'a Entry<T>,
}

impl<T> Drop for Slot<'_, T> {
    fn drop(&mut self) {
        let was_present = self.entry.is_present.swap(false, SeqCst);
        assert!(was_present);
    }
}

impl<T> Deref for Slot<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.entry.value
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
                if entry.is_present.load(SeqCst) {
                    return Some(&entry.value);
                }
            }

            self.bucket_index += 1;
            self.entry_index = 0;
        }
        None
    }
}
