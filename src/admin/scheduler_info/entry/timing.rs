use std::time::SystemTime;

use super::SchedulerEntryInfo;

impl SchedulerEntryInfo {
    /// Returns the next time the scheduler entry is due to enqueue a task.
    ///
    /// Reference: Asynq v0.26.0 public `SchedulerEntry.Next` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L901-L922>.
    pub fn next(&self) -> SystemTime {
        self.next_enqueue_at
    }

    /// Returns mutable access to the next scheduler enqueue time.
    ///
    /// Reference: Asynq v0.26.0 public `SchedulerEntry.Next` is an exported
    /// `time.Time` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L901-L922>.
    pub fn next_mut(&mut self) -> &mut SystemTime {
        &mut self.next_enqueue_at
    }
    /// Returns the next time the scheduler entry is due to enqueue a task.
    ///
    /// Reference: Asynq v0.26.0 public `SchedulerEntry.Next` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L901-L922>.
    pub fn next_enqueue_time(&self) -> SystemTime {
        self.next()
    }

    pub fn next_enqueue_time_mut(&mut self) -> &mut SystemTime {
        self.next_mut()
    }

    pub fn next_enqueue_at(&self) -> SystemTime {
        self.next_enqueue_time()
    }

    pub fn next_enqueue_at_mut(&mut self) -> &mut SystemTime {
        self.next_enqueue_time_mut()
    }

    /// Returns the previous time the scheduler entry enqueued a task.
    ///
    /// Reference: Asynq v0.26.0 public `SchedulerEntry.Prev` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L901-L922>.
    pub fn prev(&self) -> Option<SystemTime> {
        self.prev_enqueue_at
    }

    /// Returns mutable access to the previous scheduler enqueue time.
    ///
    /// Reference: Asynq v0.26.0 public `SchedulerEntry.Prev` is an exported
    /// `time.Time` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L901-L922>.
    pub fn prev_mut(&mut self) -> &mut Option<SystemTime> {
        &mut self.prev_enqueue_at
    }
    /// Returns the previous time the scheduler entry enqueued a task.
    ///
    /// Reference: Asynq v0.26.0 public `SchedulerEntry.Prev` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L901-L922>.
    pub fn prev_enqueue_time(&self) -> Option<SystemTime> {
        self.prev()
    }

    pub fn prev_enqueue_time_mut(&mut self) -> &mut Option<SystemTime> {
        self.prev_mut()
    }

    pub fn prev_enqueue_at(&self) -> Option<SystemTime> {
        self.prev_enqueue_time()
    }

    pub fn prev_enqueue_at_mut(&mut self) -> &mut Option<SystemTime> {
        self.prev_enqueue_time_mut()
    }
}
