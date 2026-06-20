//! Worker processing outcomes and errors owned by the server runtime.
//!
//! Reference: Asynq v0.26.0 processor routes each dequeued task to complete,
//! retry, archive, revoke, lease-expired recovery ownership, or idle outcomes:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L221-L381>.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use thiserror::Error;

use crate::{
    ArchiveError, CleanupError, CompleteError, DequeueError, ForwardError, LeaseError,
    LeaseExtension, RecoverError, RequeueError, ResultError, RetryError,
};

/// Result of one server worker processing attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WorkerRun {
    Completed {
        task_id: String,
    },
    Retried {
        task_id: String,
        retry_at: SystemTime,
    },
    LeaseExpired {
        task_id: String,
    },
    Archived {
        task_id: String,
    },
    Revoked {
        task_id: String,
    },
    NoProcessableTask,
}

/// Shared lease deadline state for one active server worker task.
#[derive(Debug, Clone)]
pub(crate) struct ProcessingLease {
    expires_at: Arc<Mutex<SystemTime>>,
}

impl ProcessingLease {
    pub(crate) fn new(expires_at: SystemTime) -> Self {
        Self {
            expires_at: Arc::new(Mutex::new(expires_at)),
        }
    }

    pub(crate) fn expires_at(&self) -> SystemTime {
        *self
            .expires_at
            .lock()
            .expect("server worker lease deadline poisoned")
    }

    pub(crate) fn reset(&self, extension: LeaseExtension) {
        *self
            .expires_at
            .lock()
            .expect("server worker lease deadline poisoned") = extension.expires_at();
    }

    pub(crate) fn expire_before(&self, now: SystemTime) {
        *self
            .expires_at
            .lock()
            .expect("server worker lease deadline poisoned") = now
            .checked_sub(Duration::from_nanos(1))
            .unwrap_or(UNIX_EPOCH - Duration::from_nanos(1));
    }

    pub(crate) fn is_valid_at(&self, now: SystemTime) -> bool {
        self.expires_at() >= now
    }
}

/// Error returned by worker processing and server-owned lifecycle operations.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProcessingError {
    #[error("failed to dequeue task: {0}")]
    Dequeue(#[from] DequeueError),
    #[error("failed to complete task: {0}")]
    Complete(#[from] CompleteError),
    #[error("failed to retry task: {0}")]
    Retry(#[from] RetryError),
    #[error("failed to archive task: {0}")]
    Archive(#[from] ArchiveError),
    #[error("failed to extend task lease: {0}")]
    Lease(#[from] LeaseError),
    #[error("failed to forward ready tasks: {0}")]
    Forward(#[from] ForwardError),
    #[error("failed to recover expired leases: {0}")]
    Recover(#[from] RecoverError),
    #[error("failed to cleanup expired tasks: {0}")]
    Cleanup(#[from] CleanupError),
    #[error("failed to requeue active task: {0}")]
    Requeue(#[from] RequeueError),
    #[error("failed to write task result: {0}")]
    Result(#[from] ResultError),
    #[error("{0} overflowed")]
    TimeOverflow(&'static str),
}
