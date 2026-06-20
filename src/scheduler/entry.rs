//! Scheduler entry model and metadata encoding.
//!
//! Reference: Asynq v0.26.0 public `SchedulerEntry` inspector model and
//! scheduler metadata writes:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L901-L922>
//! and <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L167-L172>.

use std::time::{Duration, SystemTime};

use crate::{
    EnqueueOptions, EnqueuePlan, SchedulerEnqueueEventInfo, SchedulerEntryInfo, Task,
    admin::{encode_scheduler_enqueue_event, encode_scheduler_entry},
};

use super::spec::{SchedulerEntrySchedule, task_options_metadata};

#[derive(Debug, Clone, PartialEq)]
pub struct SchedulerEntry {
    pub(super) id: String,
    pub(super) spec: String,
    pub(super) task: Task,
    pub(super) options: EnqueueOptions,
    pub(super) schedule: SchedulerEntrySchedule,
    pub(super) next_enqueue_at: SystemTime,
    pub(super) prev_enqueue_at: Option<SystemTime>,
}

impl SchedulerEntry {
    pub(super) fn new(
        id: String,
        spec: String,
        task: Task,
        options: EnqueueOptions,
        schedule: SchedulerEntrySchedule,
        next_enqueue_at: SystemTime,
    ) -> Self {
        Self {
            id,
            spec,
            task,
            options,
            schedule,
            next_enqueue_at,
            prev_enqueue_at: None,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn task(&self) -> &Task {
        &self.task
    }

    pub fn options(&self) -> &EnqueueOptions {
        &self.options
    }

    pub fn spec(&self) -> &str {
        &self.spec
    }

    pub fn interval(&self) -> Duration {
        match &self.schedule {
            SchedulerEntrySchedule::Every(interval) => *interval,
            SchedulerEntrySchedule::Cron { .. } => Duration::ZERO,
        }
    }

    pub fn is_cron(&self) -> bool {
        matches!(self.schedule, SchedulerEntrySchedule::Cron { .. })
    }

    pub fn next_enqueue_at(&self) -> SystemTime {
        self.next_enqueue_at
    }

    pub fn prev_enqueue_at(&self) -> Option<SystemTime> {
        self.prev_enqueue_at
    }

    pub(super) fn metadata(&self) -> Vec<u8> {
        // Reference: Asynq v0.26.0 scheduler writes `SchedulerEntry` proto
        // values through `RDB.WriteSchedulerEntries`.
        let entry = SchedulerEntryInfo::new(
            self.id.clone(),
            self.spec.clone(),
            self.task.clone(),
            task_options_metadata(&self.options),
            self.next_enqueue_at,
            self.prev_enqueue_at,
        );
        encode_scheduler_entry(Some(&entry)).expect("non-nil scheduler entry encodes")
    }

    pub(super) fn event(&self, plan: &EnqueuePlan, now: SystemTime) -> Vec<u8> {
        // Reference: Asynq v0.26.0 scheduler records
        // `SchedulerEnqueueEvent` proto values using the task id returned by
        // client enqueue, which includes any TaskID option overrides.
        // <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L167-L172>.
        let event = SchedulerEnqueueEventInfo::new(plan.message().id.clone(), now);
        encode_scheduler_enqueue_event(Some(&event))
            .expect("non-nil scheduler enqueue event encodes")
    }
}
