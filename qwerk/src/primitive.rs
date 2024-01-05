use std::{collections::HashMap, hash::Hash};

#[cfg(not(shuttle))]
pub use std::{sync, thread};

#[cfg(not(shuttle))]
pub use crossbeam_utils::Backoff;

#[cfg(shuttle)]
pub use shuttle::{sync, thread};

#[cfg(shuttle)]
#[derive(Default)]
pub struct Backoff;

#[cfg(shuttle)]
impl Backoff {
    pub fn new() -> Self {
        Default::default()
    }

    #[allow(clippy::unused_self)]
    pub fn spin(&self) {
        shuttle::thread::yield_now();
    }

    #[allow(clippy::unused_self)]
    pub fn snooze(&self) {
        shuttle::thread::yield_now();
    }
}

// shuttle-friendly version of HashIndex.
#[derive(Default)]
struct HashIndex<K, V>(shuttle::sync::RwLock<HashMap<K, V>>);

impl<K: Eq + Hash, V> HashIndex<K, V> {}
