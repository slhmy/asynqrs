use std::time::SystemTime;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::{ResultError, ResultWriter};

/// Runtime metadata for a task execution.
///
/// Reference: Asynq v0.26.0 stores task ID, queue name, retry count, and max
/// retry on the handler `context.Context`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/context/context.go#L14-L40>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskMetadata {
    task_id: String,
    queue_name: String,
    retry_count: i32,
    max_retry: i32,
}

impl TaskMetadata {
    pub fn new(
        task_id: impl Into<String>,
        queue_name: impl Into<String>,
        retry_count: i32,
        max_retry: i32,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            queue_name: queue_name.into(),
            retry_count,
            max_retry,
        }
    }

    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    pub fn retry_count(&self) -> i32 {
        self.retry_count
    }

    pub fn max_retry(&self) -> i32 {
        self.max_retry
    }
}

/// User-supplied base scope for server-owned handler invocations.
///
/// Reference: Asynq v0.26.0 derives each task handler context from a base
/// context before attaching task metadata and deadline:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L221-L235>.
#[derive(Debug, Clone, Default)]
pub struct ProcessingScope {
    deadline: Option<SystemTime>,
    cancellation: CancellationToken,
}

impl ProcessingScope {
    pub fn new(deadline: Option<SystemTime>, cancellation: CancellationToken) -> Self {
        Self {
            deadline,
            cancellation,
        }
    }

    pub fn deadline(&self) -> Option<SystemTime> {
        self.deadline
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    pub async fn cancelled(&self) {
        self.cancellation.cancelled().await;
    }
}

/// Runtime context for a single task execution.
///
/// Rust design note: this is deliberately separate from `Task`. A task
/// describes queued work, while `ProcessingContext` describes one worker
/// attempt at executing that work.
#[derive(Debug, Clone)]
pub struct ProcessingContext {
    deadline: Option<SystemTime>,
    cancellation: CancellationToken,
    parent_cancellation: Option<CancellationToken>,
    metadata: TaskMetadata,
    result_writer: Option<ResultWriter>,
}

impl ProcessingContext {
    pub fn new(
        deadline: Option<SystemTime>,
        cancellation: CancellationToken,
        metadata: TaskMetadata,
    ) -> Self {
        Self {
            deadline,
            cancellation,
            parent_cancellation: None,
            metadata,
            result_writer: None,
        }
    }

    pub fn for_task(
        deadline: Option<SystemTime>,
        cancellation: CancellationToken,
        task_id: impl Into<String>,
        queue_name: impl Into<String>,
        retry_count: i32,
        max_retry: i32,
    ) -> Self {
        Self::new(
            deadline,
            cancellation,
            TaskMetadata::new(task_id, queue_name, retry_count, max_retry),
        )
    }

    pub(crate) fn from_scope(
        scope: ProcessingScope,
        deadline: Option<SystemTime>,
        cancellation: CancellationToken,
        metadata: TaskMetadata,
    ) -> Self {
        let deadline = match (scope.deadline, deadline) {
            (Some(parent), Some(task)) => Some(parent.min(task)),
            (Some(parent), None) => Some(parent),
            (None, task) => task,
        };
        Self {
            deadline,
            cancellation,
            parent_cancellation: Some(scope.cancellation),
            metadata,
            result_writer: None,
        }
    }

    pub(crate) fn with_result_writer(mut self, writer: ResultWriter) -> Self {
        self.result_writer = Some(writer);
        self
    }

    pub fn deadline(&self) -> Option<SystemTime> {
        self.deadline
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
            || self
                .parent_cancellation
                .as_ref()
                .map(CancellationToken::is_cancelled)
                .unwrap_or(false)
    }

    pub async fn cancelled(&self) {
        match &self.parent_cancellation {
            Some(parent) => {
                tokio::select! {
                    _ = self.cancellation.cancelled() => {}
                    _ = parent.cancelled() => {}
                }
            }
            None => self.cancellation.cancelled().await,
        }
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub(crate) fn parent_cancellation_token(&self) -> Option<CancellationToken> {
        self.parent_cancellation.clone()
    }

    pub fn metadata(&self) -> &TaskMetadata {
        &self.metadata
    }

    pub fn task_id(&self) -> &str {
        self.metadata.task_id()
    }

    pub fn queue_name(&self) -> &str {
        self.metadata.queue_name()
    }

    pub fn retry_count(&self) -> i32 {
        self.metadata.retry_count()
    }

    pub fn max_retry(&self) -> i32 {
        self.metadata.max_retry()
    }

    pub fn result_writer(&self) -> Option<&ResultWriter> {
        self.result_writer.as_ref()
    }

    pub fn write_result(&self, data: impl Into<Bytes>) -> Result<usize, ResultError> {
        self.result_writer
            .as_ref()
            .ok_or(ResultError::NoResultWriter)?
            .write(data)
    }

    /// Writes result data and waits until the worker persists it through the
    /// broker.
    ///
    /// Reference: Asynq v0.26.0 handlers can write task results through the
    /// task result writer, which returns the broker write result:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L555-L572>.
    pub async fn write_result_async(&self, data: impl Into<Bytes>) -> Result<usize, ResultError> {
        self.result_writer
            .as_ref()
            .ok_or(ResultError::NoResultWriter)?
            .write_async(data)
            .await
    }
}
