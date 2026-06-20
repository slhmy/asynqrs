use std::time::SystemTime;

use async_trait::async_trait;

use crate::server::ServerMaintenanceRun;
use crate::task::TaskMessage;
use crate::{CleanupError, ForwardError, LeaseError, LeaseExtension, RecoverError};

/// Async broker interface for extending active task leases.
///
/// Reference: Asynq v0.26.0 `RDB.ExtendLease` updates active-task lease
/// scores and returns the new expiration time:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1355-L1374>.
#[async_trait]
pub(crate) trait LeaseBroker {
    async fn extend_leases(
        &mut self,
        queue: &str,
        task_ids: &[String],
    ) -> Result<LeaseExtension, LeaseError>;
}

/// Async broker interface for moving due scheduled/retry tasks back to pending.
///
/// Reference: Asynq v0.26.0 forwarder calls `Broker.ForwardIfReady` once
/// with all configured queues, and `RDB.ForwardIfReady` owns queue iteration:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/forwarder.go#L61-L65>.
#[async_trait]
pub(crate) trait ForwardBroker {
    async fn forward_if_ready(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ForwardError>;
}

/// Async broker interface for recovering active tasks whose leases expired.
///
/// Reference: Asynq v0.26.0 recoverer asks the broker to list
/// active tasks whose leases expired before its 30-second clock-skew cutoff,
/// then sends them through retry/archive lifecycle paths:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/recoverer.go#L72-L87>.
#[async_trait]
pub(crate) trait RecoverBroker {
    async fn list_lease_expired(
        &mut self,
        cutoff: SystemTime,
        queues: &[String],
    ) -> Result<Vec<TaskMessage>, RecoverError>;
}

/// Async broker interface for removing expired completed tasks from one queue.
///
/// Reference: Asynq v0.26.0 `RDB.DeleteExpiredCompletedTasks`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1278-L1290>.
#[async_trait]
pub(crate) trait CleanupBroker {
    async fn delete_expired_completed_tasks(
        &mut self,
        queue: &str,
        batch_size: isize,
    ) -> Result<usize, CleanupError>;
}
