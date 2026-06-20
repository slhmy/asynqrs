use std::time::Duration;

use super::super::SchedulerError;
use super::super::core::Scheduler;
use super::super::spec::{format_duration_spec, generated_scheduler_entry_id};
use crate::{EnqueueOptions, Task};

impl<B, C> Scheduler<B, C>
where
    C: crate::client::Clock,
{
    pub fn register(
        &mut self,
        entry_id: impl Into<String>,
        task: Task,
        interval: Duration,
    ) -> Result<(), SchedulerError> {
        self.register_with(entry_id, task, interval, EnqueueOptions::default())
    }

    /// Registers a fixed-interval scheduler entry and returns a generated
    /// entry id.
    ///
    /// Reference: Asynq v0.26.0 `Scheduler.Register` creates a UUID-backed
    /// scheduler entry id and returns it for later `Unregister` calls:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L201-L225>.
    pub fn register_with_generated_id(
        &mut self,
        task: Task,
        interval: Duration,
    ) -> Result<String, SchedulerError> {
        self.register_with_generated_id_and(task, interval, EnqueueOptions::default())
    }

    /// Registers a fixed-interval scheduler entry with enqueue options.
    ///
    /// Reference: Asynq v0.26.0 `Scheduler.Register` accepts per-entry
    /// enqueue `Option` values and passes them to `Client.Enqueue` on each run:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L201-L222>.
    pub fn register_with(
        &mut self,
        entry_id: impl Into<String>,
        task: Task,
        interval: Duration,
        options: EnqueueOptions,
    ) -> Result<(), SchedulerError> {
        let spec = format!("@every {}", format_duration_spec(interval)?);
        self.register_every(entry_id, task, spec, interval, options)
    }

    /// Registers a fixed-interval scheduler entry with enqueue options and
    /// returns a generated entry id.
    ///
    /// Reference: Asynq v0.26.0 stores generated scheduler entry ids in an
    /// unregister map keyed by that id:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L217-L244>.
    pub fn register_with_generated_id_and(
        &mut self,
        task: Task,
        interval: Duration,
        options: EnqueueOptions,
    ) -> Result<String, SchedulerError> {
        let entry_id = generated_scheduler_entry_id();
        self.register_with(entry_id.clone(), task, interval, options)?;
        Ok(entry_id)
    }
}
