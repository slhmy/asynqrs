use std::time::SystemTime;

use crate::task::TaskMessage;
use crate::{ArchiveError, CompleteError, DequeueError, DequeuedTask, ResultError, RetryError};

/// Allocation-light worker broker path for server-owned worker loops.
///
/// This internal trait is the Rust-native counterpart to the dyn-compatible
/// broker traits. It keeps worker runtime code on crate-owned async trait
/// boundaries without exposing this hot path as a user extension surface.
#[async_trait::async_trait]
pub(crate) trait WorkerBrokerCore {
    async fn dequeue(&mut self, queues: &[String]) -> Result<DequeuedTask, DequeueError>;

    async fn complete(&mut self, message: &TaskMessage) -> Result<(), CompleteError>;

    async fn retry(
        &mut self,
        message: &TaskMessage,
        retry_at: SystemTime,
        error_message: &str,
        is_failure: bool,
    ) -> Result<(), RetryError>;

    async fn archive(
        &mut self,
        message: &TaskMessage,
        error_message: &str,
    ) -> Result<(), ArchiveError>;

    async fn write_result(
        &mut self,
        queue: &str,
        task_id: &str,
        data: Vec<u8>,
    ) -> Result<usize, ResultError>;
}
