use std::time::SystemTime;

use crate::Task;

mod identity;
mod task;
mod timing;

/// Reference: Asynq v0.26.0 public `SchedulerEntry` inspector model:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L901-L922>.
#[derive(Debug, Clone, PartialEq)]
pub struct SchedulerEntryInfo {
    id: String,
    spec: String,
    task: Task,
    enqueue_options: Vec<String>,
    next_enqueue_at: SystemTime,
    prev_enqueue_at: Option<SystemTime>,
}

/// Public scheduler-entry inspection model name matching upstream Asynq.
///
/// Reference: Asynq v0.26.0 public `SchedulerEntry` inspector model:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L901-L922>.
///
/// Rust compatibility: crate-root `SchedulerEntry` still names the scheduler
/// runtime registration type, so this inspector model is also exported as
/// `InspectorSchedulerEntry` at the crate root.
pub type SchedulerEntry = SchedulerEntryInfo;

impl SchedulerEntryInfo {
    pub fn new(
        id: String,
        spec: String,
        task: Task,
        enqueue_options: Vec<String>,
        next_enqueue_at: SystemTime,
        prev_enqueue_at: Option<SystemTime>,
    ) -> Self {
        Self {
            id,
            spec,
            task,
            enqueue_options,
            next_enqueue_at,
            prev_enqueue_at,
        }
    }
}
