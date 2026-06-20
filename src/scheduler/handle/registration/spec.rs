use tokio::sync::oneshot;

use crate::scheduler::spec::generated_scheduler_entry_id;
use crate::{EnqueueOptions, Task};

use super::super::super::SchedulerError;
use super::super::{SchedulerCommand, SchedulerHandle};

impl SchedulerHandle {
    /// Registers a scheduler entry from an upstream-style spec string on a
    /// running background scheduler.
    ///
    /// Reference: Asynq v0.26.0 `Scheduler.Register` delegates to robfig/cron,
    /// whose entries can be added after `Start`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L217-L222>.
    pub async fn register_spec(
        &self,
        entry_id: impl Into<String>,
        task: Task,
        spec: impl Into<String>,
    ) -> Result<(), SchedulerError> {
        self.register_spec_with(entry_id, task, spec, EnqueueOptions::default())
            .await
    }

    /// Registers a scheduler entry from an upstream-style spec string on a
    /// running background scheduler and returns a generated entry id.
    ///
    /// Reference: Asynq v0.26.0 `Scheduler.Register` returns a generated id
    /// for the newly added entry:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L201-L225>.
    pub async fn register_spec_with_generated_id(
        &self,
        task: Task,
        spec: impl Into<String>,
    ) -> Result<String, SchedulerError> {
        self.register_spec_with_generated_id_and(task, spec, EnqueueOptions::default())
            .await
    }

    /// Registers a scheduler entry from an upstream-style spec string with
    /// enqueue options on a running background scheduler.
    ///
    /// Reference: Asynq v0.26.0 keeps scheduler registration mutable while the
    /// background scheduler is active:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L201-L244>.
    pub async fn register_spec_with(
        &self,
        entry_id: impl Into<String>,
        task: Task,
        spec: impl Into<String>,
        options: EnqueueOptions,
    ) -> Result<(), SchedulerError> {
        let (result_tx, result_rx) = oneshot::channel();
        let command = SchedulerCommand::Register {
            entry_id: entry_id.into(),
            task,
            spec: spec.into(),
            options,
            result: result_tx,
        };
        self.commands
            .send(command)
            .await
            .map_err(|_| SchedulerError::AlreadyStopped)?;
        result_rx
            .await
            .unwrap_or(Err(SchedulerError::AlreadyStopped))
    }

    /// Registers a scheduler entry from an upstream-style spec string with
    /// enqueue options on a running background scheduler and returns a
    /// generated entry id.
    ///
    /// Reference: Asynq v0.26.0 keeps generated entry ids in its unregister
    /// map for scheduler entries added before or after `Start`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L217-L244>.
    pub async fn register_spec_with_generated_id_and(
        &self,
        task: Task,
        spec: impl Into<String>,
        options: EnqueueOptions,
    ) -> Result<String, SchedulerError> {
        let entry_id = generated_scheduler_entry_id();
        self.register_spec_with(entry_id.clone(), task, spec, options)
            .await?;
        Ok(entry_id)
    }
}
