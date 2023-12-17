//! A single-producer single-consumer channel for sending a simple signal.

use parking_lot::{Condvar, Mutex};
use std::{sync::Arc, time::Duration};

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
    /// Waits for a signal to be received from the channel, but only for
    /// a limited time.
    ///
    /// Returns `true` if the signal was received, or `false` if the timeout
    /// elapsed.
    ///
    /// Once the signal is received, this method will always return `true`
    /// immediately in subsequent calls.
    ///
    /// If the `Sender` is dropped without sending a signal, this method will
    /// block forever.
    pub fn recv_timeout(&self, timeout: Duration) -> bool {
        let mut guard = self.shared.mutex.lock();
        !self
            .shared
            .condvar
            .wait_while_for(&mut guard, |signaled| !*signaled, timeout)
            .timed_out()
    }
}
