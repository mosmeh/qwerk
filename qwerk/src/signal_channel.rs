//! A single-producer single-consumer channel for sending a simple signal.

use parking_lot::{Condvar, Mutex};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

pub fn channel() -> (Sender, Receiver) {
    let shared = Arc::new(Shared::default());
    (
        Sender {
            shared: shared.clone(),
        },
        Receiver { shared },
    )
}

#[derive(Default)]
struct Shared {
    mutex: Mutex<bool>,
    condvar: Condvar,
}

pub struct Sender {
    shared: Arc<Shared>,
}

impl Sender {
    /// Sends a signal.
    pub fn send(&self) {
        *self.shared.mutex.lock() = true;
        self.shared.condvar.notify_one();
    }
}

pub struct Receiver {
    shared: Arc<Shared>,
}

impl Receiver {
    /// Creates an iterator that yields with `interval` until a signal is
    /// received from the channel.
    ///
    /// If a tick is missed, the iterator will yield immediately, and the next
    /// tick will be scheduled `interval` after the yield.
    pub fn tick(self, interval: Duration) -> Tick {
        Tick {
            receiver: self,
            interval,
            deadline: Instant::now() + interval,
        }
    }

    fn recv_until(&self, deadline: Instant) -> bool {
        let mut guard = self.shared.mutex.lock();
        !self
            .shared
            .condvar
            .wait_while_until(&mut guard, |signaled| !*signaled, deadline)
            .timed_out()
    }

    pub fn try_recv(&self) -> bool {
        *self.shared.mutex.lock()
    }
}

pub struct Tick {
    receiver: Receiver,
    interval: Duration,
    deadline: Instant,
}

impl Iterator for Tick {
    type Item = Instant;

    fn next(&mut self) -> Option<Self::Item> {
        if self.receiver.recv_until(self.deadline) {
            return None;
        }
        let prev = self.deadline;
        self.deadline = Instant::now() + self.interval;
        Some(prev)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    #[test]
    fn tick() {
        let (tx, rx) = super::channel();
        let mut tick = rx.tick(Duration::from_millis(0));
        assert!(tick.next().is_some());
        assert!(tick.next().is_some());
        tx.send();
        assert!(tick.next().is_none());
    }
}
