//! Server-owned lifecycle sync backlog.
//!
//! Reference: Asynq v0.26.0 processor routes failed complete, retry, and
//! archive writes through a syncer so worker results stay successful while
//! Redis lifecycle state is retried:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L263-L390>.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::SystemTime,
};

use crate::task::TaskMessage;

use super::logging::log_warn;
use super::{LogLevel, Logger, WorkerBrokerCore};

/// Shared lifecycle sync backlog used by worker handles and the server syncer.
///
/// Reference: Asynq v0.26.0 keeps failed lifecycle writes in a processor syncer
/// queue that can be retried outside the worker result path:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/syncer.go>.
#[derive(Debug, Clone, Default)]
pub(crate) struct PendingSyncBacklog {
    queue: Arc<Mutex<VecDeque<PendingSyncOperation>>>,
}

impl PendingSyncBacklog {
    pub(crate) fn push(&self, operation: PendingSyncOperation) {
        self.queue
            .lock()
            .expect("server pending sync queue poisoned")
            .push_back(operation);
    }

    pub(crate) fn pop(&self) -> Option<PendingSyncOperation> {
        self.queue
            .lock()
            .expect("server pending sync queue poisoned")
            .pop_front()
    }

    pub(crate) fn len(&self) -> usize {
        self.queue
            .lock()
            .expect("server pending sync queue poisoned")
            .len()
    }

    pub(crate) fn enqueue_complete(
        &self,
        message: TaskMessage,
        deadline: SystemTime,
        sync_error_message: String,
    ) {
        self.push(PendingSyncOperation::Complete {
            message,
            deadline,
            sync_error_message,
        });
    }

    pub(crate) fn enqueue_retry(
        &self,
        message: TaskMessage,
        retry_at: SystemTime,
        error_message: String,
        is_failure: bool,
        deadline: SystemTime,
        sync_error_message: String,
    ) {
        self.push(PendingSyncOperation::Retry {
            message,
            retry_at,
            error_message,
            is_failure,
            deadline,
            sync_error_message,
        });
    }

    pub(crate) fn enqueue_archive(
        &self,
        message: TaskMessage,
        error_message: String,
        deadline: SystemTime,
        sync_error_message: String,
    ) {
        self.push(PendingSyncOperation::Archive {
            message,
            error_message,
            deadline,
            sync_error_message,
        });
    }

    pub(crate) async fn drain<B, N>(
        &self,
        broker: &mut B,
        mut now: N,
        policy: PendingSyncDrainPolicy,
        logger: &Option<Arc<dyn Logger>>,
        log_level: LogLevel,
    ) where
        B: WorkerBrokerCore + Send,
        N: FnMut() -> SystemTime,
    {
        let pending = self.len();
        for _ in 0..pending {
            let operation = self.pop();
            let Some(operation) = operation else {
                break;
            };
            // Reference: Asynq v0.26.0 `syncer` drops stale lifecycle sync
            // requests once their worker lease deadline has elapsed:
            // <https://github.com/hibiken/asynq/blob/v0.26.0/syncer.go#L68-L71>.
            if policy.drop_stale() && operation.deadline() < now() {
                continue;
            }
            if Self::apply_operation(broker, &operation).await.is_err() {
                if policy.log_final_failure() {
                    // Reference: Asynq v0.26.0 periodic sync retries keep
                    // failed requests queued silently, while shutdown tries
                    // each request one final time, logs the original sync
                    // request error, and then exits without requeueing:
                    // <https://github.com/hibiken/asynq/blob/v0.26.0/syncer.go#L53-L76>.
                    log_warn(
                        logger,
                        log_level,
                        format_args!("{}", operation.sync_error_message()),
                    );
                }
                if policy.requeue_failures() {
                    self.push(operation);
                }
            }
        }
    }

    pub(crate) async fn apply_operation<B>(
        broker: &mut B,
        operation: &PendingSyncOperation,
    ) -> Result<(), ()>
    where
        B: WorkerBrokerCore + Send,
    {
        match operation {
            PendingSyncOperation::Complete { message, .. } => {
                WorkerBrokerCore::complete(broker, message)
                    .await
                    .map_err(|_| ())
            }
            PendingSyncOperation::Retry {
                message,
                retry_at,
                error_message,
                is_failure,
                ..
            } => WorkerBrokerCore::retry(broker, message, *retry_at, error_message, *is_failure)
                .await
                .map_err(|_| ()),
            PendingSyncOperation::Archive {
                message,
                error_message,
                ..
            } => WorkerBrokerCore::archive(broker, message, error_message)
                .await
                .map_err(|_| ()),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum PendingSyncOperation {
    Complete {
        message: TaskMessage,
        deadline: SystemTime,
        sync_error_message: String,
    },
    Retry {
        message: TaskMessage,
        retry_at: SystemTime,
        error_message: String,
        is_failure: bool,
        deadline: SystemTime,
        sync_error_message: String,
    },
    Archive {
        message: TaskMessage,
        error_message: String,
        deadline: SystemTime,
        sync_error_message: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PendingSyncDrainPolicy {
    Periodic,
    ShutdownFinal,
}

impl PendingSyncDrainPolicy {
    pub(crate) fn drop_stale(self) -> bool {
        matches!(self, Self::Periodic)
    }

    pub(crate) fn requeue_failures(self) -> bool {
        matches!(self, Self::Periodic)
    }

    pub(crate) fn log_final_failure(self) -> bool {
        matches!(self, Self::ShutdownFinal)
    }
}

impl PendingSyncOperation {
    pub(crate) fn deadline(&self) -> SystemTime {
        match self {
            Self::Complete { deadline, .. }
            | Self::Retry { deadline, .. }
            | Self::Archive { deadline, .. } => *deadline,
        }
    }

    pub(crate) fn sync_error_message(&self) -> &str {
        match self {
            Self::Complete {
                sync_error_message, ..
            }
            | Self::Retry {
                sync_error_message, ..
            }
            | Self::Archive {
                sync_error_message, ..
            } => sync_error_message,
        }
    }
}
