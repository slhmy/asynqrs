use crate::Task;

use super::SchedulerEntryInfo;

impl SchedulerEntryInfo {
    pub fn task(&self) -> &Task {
        &self.task
    }

    /// Returns mutable access to the scheduler entry task.
    ///
    /// Reference: Asynq v0.26.0 public `SchedulerEntry.Task` is an exported
    /// task pointer field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L901-L922>.
    pub fn task_mut(&mut self) -> &mut Task {
        &mut self.task
    }
    pub fn enqueue_options(&self) -> &[String] {
        &self.enqueue_options
    }

    /// Returns mutable access to the raw scheduler-entry enqueue options.
    ///
    /// Reference: Asynq v0.26.0 public `SchedulerEntry.EnqueueOptions` is an
    /// exported string-slice field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L901-L922>.
    pub fn enqueue_options_mut(&mut self) -> &mut Vec<String> {
        &mut self.enqueue_options
    }
}
