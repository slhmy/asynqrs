use std::time::SystemTime;

use crate::task::TaskMessage;
use crate::{EnqueuePlan, TaskState};

mod fields;
mod timing;

/// Redis-backed task inspection model.
///
/// Reference: Asynq v0.26.0 `TaskInfo` inspector model:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go>.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskInfo {
    pub(super) message: TaskMessage,
    pub(super) max_retry: i64,
    pub(super) state: TaskState,
    pub(super) is_orphaned: bool,
    pub(super) next_process_at: Option<SystemTime>,
    pub(super) result: Vec<u8>,
}

impl TaskInfo {
    /// Builds the task metadata returned by the client enqueue path.
    ///
    /// Reference: Asynq v0.26.0 `Client.EnqueueContext` returns `newTaskInfo`
    /// for the enqueued task message:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L316-L331>.
    pub fn from_enqueue_plan(plan: &EnqueuePlan) -> Self {
        let mut info = Self::new(
            plan.message().clone(),
            plan.state(),
            false,
            plan.next_process_at(),
            Vec::new(),
        );
        // Reference: Asynq v0.26.0 `newTaskInfo` reads `msg.Retry` from the
        // pre-encode Go `TaskMessage`, before protobuf `int32` conversion:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L316-L331>
        // <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L166-L180>.
        info.max_retry = i64::try_from(plan.max_retry()).unwrap_or(i64::MAX);
        info
    }

    pub(crate) fn new(
        message: TaskMessage,
        state: TaskState,
        is_orphaned: bool,
        next_process_at: Option<SystemTime>,
        result: Vec<u8>,
    ) -> Self {
        let max_retry = i64::from(message.retry);
        Self {
            message,
            max_retry,
            state,
            is_orphaned,
            next_process_at,
            result,
        }
    }
}
