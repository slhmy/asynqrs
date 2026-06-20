use async_trait::async_trait;

use crate::CancelError;

/// Internal broker interface for broadcasting active task cancellation.
///
/// Reference: Asynq v0.26.0 `Inspector.CancelProcessing` publishes task ids
/// onto the Redis `asynq:cancel` channel listened to by processors:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go>.
#[async_trait]
pub trait CancelBroker {
    async fn publish_cancel(&mut self, task_id: &str) -> Result<usize, CancelError>;
}
