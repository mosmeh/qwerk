use std::ptr::NonNull;

/// A wrapper around a [`NonNull<T>`] that indicates that the pointee is
/// a heap-allocated shared object.
pub struct Shared<T>(NonNull<T>);

impl<T> std::fmt::Debug for Shared<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Shared").field(&self.0).finish()
    }
}

impl<T> Clone for Shared<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Shared<T> {}

unsafe impl<T: Sync + Send> Send for Shared<T> {}
unsafe impl<T: Sync + Send> Sync for Shared<T> {}

impl<T> From<NonNull<T>> for Shared<T> {
    fn from(ptr: NonNull<T>) -> Self {
        Self(ptr)
    }
}

impl<T> Shared<T> {
    pub fn new(value: T) -> Self {
        Self(Box::leak(Box::new(value)).into())
    }

    pub unsafe fn drop_in_place(self) {
        let _ = Box::from_raw(self.0.as_ptr());
    }

    pub const unsafe fn as_ref<'a>(self) -> &'a T {
        self.0.as_ref()
    }
}
