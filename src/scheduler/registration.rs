use std::time::Duration;

use super::SchedulerError;
use super::core::Scheduler;
use super::entry::SchedulerEntry;
use super::spec::SchedulerEntrySchedule;
use crate::{EnqueueOptions, Task};

mod fixed;
mod spec;

impl<B, C> Scheduler<B, C>
where
    C: crate::client::Clock,
{
    pub(super) fn register_every(
        &mut self,
        entry_id: impl Into<String>,
        task: Task,
        spec: String,
        interval: Duration,
        options: EnqueueOptions,
    ) -> Result<(), SchedulerError> {
        self.register_schedule(
            entry_id,
            task,
            spec,
            SchedulerEntrySchedule::Every(interval),
            options,
        )
    }

    pub(in crate::scheduler) fn register_schedule(
        &mut self,
        entry_id: impl Into<String>,
        task: Task,
        spec: String,
        schedule: SchedulerEntrySchedule,
        options: EnqueueOptions,
    ) -> Result<(), SchedulerError> {
        let entry_id = entry_id.into();
        if entry_id.trim().is_empty() {
            return Err(SchedulerError::EmptyEntryId);
        }
        if matches!(schedule, SchedulerEntrySchedule::Every(interval) if interval.is_zero()) {
            return Err(SchedulerError::ZeroInterval);
        }
        let next_enqueue_at = schedule.next_after(self.clock.now(), &spec)?;
        self.entries.push(SchedulerEntry::new(
            entry_id,
            spec,
            task,
            options,
            schedule,
            next_enqueue_at,
        ));
        Ok(())
    }
}
