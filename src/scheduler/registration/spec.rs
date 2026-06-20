use super::super::SchedulerError;
use super::super::core::Scheduler;
use super::super::spec::{generated_scheduler_entry_id, parse_scheduler_spec};
use crate::{EnqueueOptions, Task};

impl<B, C> Scheduler<B, C>
where
    C: crate::client::Clock,
{
    /// Registers a scheduler entry from an upstream-style spec string.
    ///
    /// Reference: Asynq v0.26.0 scheduler entries carry a cron spec string in
    /// metadata and support `@every` interval specs through the cron parser:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go>.
    pub fn register_spec(
        &mut self,
        entry_id: impl Into<String>,
        task: Task,
        spec: impl Into<String>,
    ) -> Result<(), SchedulerError> {
        self.register_spec_with(entry_id, task, spec, EnqueueOptions::default())
    }

    /// Registers a scheduler entry from an upstream-style spec string and
    /// returns a generated entry id.
    ///
    /// Reference: Asynq v0.26.0 `Scheduler.Register` returns the newly
    /// generated scheduler entry id:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L201-L225>.
    pub fn register_spec_with_generated_id(
        &mut self,
        task: Task,
        spec: impl Into<String>,
    ) -> Result<String, SchedulerError> {
        self.register_spec_with_generated_id_and(task, spec, EnqueueOptions::default())
    }

    /// Registers a scheduler entry from an upstream-style spec string with
    /// enqueue options.
    ///
    /// Reference: Asynq v0.26.0 `Scheduler.Register` stores the task and opts
    /// on its enqueue job and reuses them on every scheduled enqueue:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L201-L222>.
    pub fn register_spec_with(
        &mut self,
        entry_id: impl Into<String>,
        task: Task,
        spec: impl Into<String>,
        options: EnqueueOptions,
    ) -> Result<(), SchedulerError> {
        let spec = spec.into();
        let schedule = parse_scheduler_spec(&spec, self.timezone)?;
        self.register_schedule(entry_id, task, spec, schedule, options)
    }

    /// Registers a scheduler entry from an upstream-style spec string with
    /// enqueue options and returns a generated entry id.
    ///
    /// Reference: Asynq v0.26.0 uses the generated entry id as the public
    /// handle for metadata, enqueue history, and `Unregister`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L201-L244>.
    pub fn register_spec_with_generated_id_and(
        &mut self,
        task: Task,
        spec: impl Into<String>,
        options: EnqueueOptions,
    ) -> Result<String, SchedulerError> {
        let entry_id = generated_scheduler_entry_id();
        self.register_spec_with(entry_id.clone(), task, spec, options)?;
        Ok(entry_id)
    }
}
