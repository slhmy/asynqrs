use std::time::SystemTime;

use async_trait::async_trait;

use crate::task::TaskMessage;
use crate::{ArchiveError, RequeueError, RetryError};

/// Worker lifecycle broker traits shared outside the hot worker loop.
///
/// Reference: Asynq v0.26.0 internal `base.Broker` worker-related methods:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L371-L419>.
///
/// `WorkerBrokerCore` owns dequeue, complete, and result writes for the worker
/// hot path. These async-trait facades remain for lifecycle operations reused by
/// maintenance and shutdown code.
/// Async broker interface for retrying a failed active task.
///
/// Reference: Asynq v0.26.0 `RDB.Retry`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L380-L418>.
#[async_trait]
pub(crate) trait RetryBroker {
    async fn retry(
        &mut self,
        message: &TaskMessage,
        retry_at: SystemTime,
        error_message: &str,
        is_failure: bool,
    ) -> Result<(), RetryError>;
}

/// Async broker interface for archiving a failed active task.
///
/// Reference: Asynq v0.26.0 `RDB.Archive` archives failed active tasks using
/// the broker clock and without accepting an archive time or `isFailure`
/// option:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.
#[async_trait]
pub(crate) trait ArchiveBroker {
    async fn archive(
        &mut self,
        message: &TaskMessage,
        error_message: &str,
    ) -> Result<(), ArchiveError>;
}

/// Async broker interface for moving an active task back to pending during
/// worker shutdown.
///
/// Reference: Asynq v0.26.0 `RDB.Requeue`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L486-L506>.
#[async_trait]
pub(crate) trait RequeueBroker {
    async fn requeue(&mut self, message: &TaskMessage) -> Result<(), RequeueError>;
}
