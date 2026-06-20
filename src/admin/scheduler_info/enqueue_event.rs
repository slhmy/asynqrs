use std::time::SystemTime;

/// Reference: Asynq v0.26.0 public `SchedulerEnqueueEvent` inspector model:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L1026-L1034>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerEnqueueEventInfo {
    task_id: String,
    enqueued_at: SystemTime,
}

/// Public scheduler enqueue event inspection model name matching upstream Asynq.
///
/// Reference: Asynq v0.26.0 public `SchedulerEnqueueEvent` inspector model:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L1026-L1034>.
pub type SchedulerEnqueueEvent = SchedulerEnqueueEventInfo;

impl SchedulerEnqueueEventInfo {
    pub fn new(task_id: String, enqueued_at: SystemTime) -> Self {
        Self {
            task_id,
            enqueued_at,
        }
    }

    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    /// Returns mutable access to the scheduler enqueue event task ID.
    ///
    /// Reference: Asynq v0.26.0 public `SchedulerEnqueueEvent.TaskID` is an
    /// exported string field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L1026-L1034>.
    pub fn task_id_mut(&mut self) -> &mut String {
        &mut self.task_id
    }
    pub fn enqueued_at(&self) -> SystemTime {
        self.enqueued_at
    }

    /// Returns mutable access to the scheduler enqueue event time.
    ///
    /// Reference: Asynq v0.26.0 public `SchedulerEnqueueEvent.EnqueuedAt` is
    /// an exported `time.Time` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L1026-L1034>.
    pub fn enqueued_at_mut(&mut self) -> &mut SystemTime {
        &mut self.enqueued_at
    }
}
