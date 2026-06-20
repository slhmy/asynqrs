use std::time::Duration;

use tokio::sync::oneshot;

use crate::scheduler::spec::{format_duration_spec, generated_scheduler_entry_id};
use crate::{EnqueueOptions, Task};

use super::super::super::SchedulerError;
use super::super::{SchedulerCommand, SchedulerHandle};

impl SchedulerHandle {
    /// Registers a fixed-interval scheduler entry on a running background
    /// scheduler.
    ///
    /// Reference: Asynq v0.26.0 `Scheduler.Register` can be called while the
    /// scheduler is running because the underlying cron scheduler supports
    /// runtime entry mutation:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L201-L222>.
    pub async fn register(
        &self,
        entry_id: impl Into<String>,
        task: Task,
        interval: Duration,
    ) -> Result<(), SchedulerError> {
        self.register_with(entry_id, task, interval, EnqueueOptions::default())
            .await
    }

    /// Registers a fixed-interval scheduler entry on a running background
    /// scheduler and returns a generated entry id.
    ///
    /// Reference: Asynq v0.26.0 `Scheduler.Register` can add entries while
    /// running and returns the generated entry id:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L201-L225>.
    pub async fn register_with_generated_id(
        &self,
        task: Task,
        interval: Duration,
    ) -> Result<String, SchedulerError> {
        self.register_with_generated_id_and(task, interval, EnqueueOptions::default())
            .await
    }

    /// Registers a fixed-interval scheduler entry with enqueue options on a
    /// running background scheduler.
    ///
    /// Reference: Asynq v0.26.0 `Scheduler.Register` stores per-entry enqueue
    /// options and applies them on future scheduled enqueues:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L201-L222>.
    pub async fn register_with(
        &self,
        entry_id: impl Into<String>,
        task: Task,
        interval: Duration,
        options: EnqueueOptions,
    ) -> Result<(), SchedulerError> {
        let spec = format!("@every {}", format_duration_spec(interval)?);
        let (result_tx, result_rx) = oneshot::channel();
        let command = SchedulerCommand::RegisterEvery {
            entry_id: entry_id.into(),
            task,
            spec,
            interval,
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

    /// Registers a fixed-interval scheduler entry with enqueue options on a
    /// running background scheduler and returns a generated entry id.
    ///
    /// Reference: Asynq v0.26.0 returns the generated scheduler entry id so it
    /// can be passed to `Scheduler.Unregister`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L201-L244>.
    pub async fn register_with_generated_id_and(
        &self,
        task: Task,
        interval: Duration,
        options: EnqueueOptions,
    ) -> Result<String, SchedulerError> {
        let entry_id = generated_scheduler_entry_id();
        self.register_with(entry_id.clone(), task, interval, options)
            .await?;
        Ok(entry_id)
    }
}
