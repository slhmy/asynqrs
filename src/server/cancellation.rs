use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio_util::sync::CancellationToken;

/// Cancels active tasks owned by a server in the current process.
///
/// Reference: Asynq v0.26.0 server cancellation listens on the Redis
/// `asynq:cancel` pub/sub channel and cancels the task context:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go>.
///
/// Rust-native note: cancellation tokens are registered in server-owned runtime
/// state and shared with the Redis cancellation listener.
#[derive(Debug, Clone, Default)]
pub struct ServerCanceller {
    active: Arc<Mutex<HashMap<String, CancellationToken>>>,
}

impl ServerCanceller {
    /// Requests cancellation for an active task id.
    ///
    /// Returns `true` when the task is currently registered by this server.
    pub fn cancel(&self, task_id: &str) -> bool {
        let sender = self
            .active
            .lock()
            .expect("server cancellation registry poisoned")
            .get(task_id)
            .cloned();

        sender
            .map(|token| {
                token.cancel();
                true
            })
            .unwrap_or(false)
    }

    pub(crate) fn register(&self, task_id: &str) -> CancellationToken {
        let token = CancellationToken::new();
        self.active
            .lock()
            .expect("server cancellation registry poisoned")
            .insert(task_id.to_owned(), token.clone());
        token
    }

    pub(crate) fn unregister(&self, task_id: &str) {
        self.active
            .lock()
            .expect("server cancellation registry poisoned")
            .remove(task_id);
    }
}
