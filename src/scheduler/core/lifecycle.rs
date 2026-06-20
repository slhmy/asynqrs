use super::Scheduler;

use crate::scheduler::broker::SchedulerBroker;
use crate::scheduler::logging::{
    log_scheduler_debug, log_scheduler_error, log_scheduler_info, log_scheduler_warn,
};
use crate::scheduler::{SchedulerError, SchedulerState};

impl<B, C> Scheduler<B, C> {
    /// Removes a registered scheduler entry by entry id.
    ///
    /// Reference: Asynq v0.26.0 `Scheduler.Unregister` removes an entry from
    /// the cron scheduler and returns an error for unknown entry ids without a
    /// separate empty-id precondition:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L233-L244>.
    pub fn unregister(&mut self, entry_id: &str) -> Result<(), SchedulerError> {
        let before_len = self.entries.len();
        self.entries.retain(|entry| entry.id != entry_id);
        if self.entries.len() == before_len {
            return Err(SchedulerError::EntryNotFound(entry_id.to_owned()));
        }
        Ok(())
    }

    pub(in crate::scheduler) fn begin_start(&mut self) -> Result<(), SchedulerError> {
        match self.state {
            SchedulerState::New => {
                self.state = SchedulerState::Active;
                Ok(())
            }
            SchedulerState::Active => Err(SchedulerError::AlreadyRunning),
            SchedulerState::Closed => Err(SchedulerError::AlreadyStopped),
        }
    }

    pub(in crate::scheduler) fn close(&mut self)
    where
        B: SchedulerBroker,
    {
        self.state = SchedulerState::Closed;
        if !self.shared_connection {
            self.broker.close();
        }
    }

    pub(in crate::scheduler) fn begin_shutdown(&mut self) -> bool {
        match self.state {
            SchedulerState::New | SchedulerState::Closed => false,
            SchedulerState::Active => {
                self.state = SchedulerState::Closed;
                true
            }
        }
    }

    /// Checks the scheduler backing store connection.
    ///
    /// Reference: Asynq v0.26.0 `Scheduler.Ping` returns successfully after
    /// shutdown and otherwise delegates to Redis `RDB.Ping`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L367-L375>.
    pub async fn ping(&mut self) -> Result<(), SchedulerError>
    where
        B: SchedulerBroker + Send,
    {
        if self.state == SchedulerState::Closed {
            return Ok(());
        }
        self.broker.ping().await.map_err(SchedulerError::Ping)
    }

    /// Stops an active scheduler and clears runtime scheduler metadata.
    ///
    /// Reference: Asynq v0.26.0 `Scheduler.Shutdown` is a no-op for new or
    /// already closed schedulers; active schedulers transition to closed and
    /// clear scheduler entries plus per-entry history:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L309-L334>.
    pub async fn shutdown(&mut self) -> Result<(), SchedulerError>
    where
        B: SchedulerBroker + Send,
    {
        if !self.begin_shutdown() {
            return Ok(());
        }
        log_scheduler_info(
            &self.logger,
            self.log_level,
            format_args!("Scheduler shutting down"),
        );
        let result = self.clear_runtime_metadata().await;
        if result.is_ok() {
            log_scheduler_info(
                &self.logger,
                self.log_level,
                format_args!("Scheduler stopped"),
            );
        }
        if !self.shared_connection {
            self.broker.close();
        }
        result
    }
}

impl<B, C> Scheduler<B, C>
where
    B: SchedulerBroker + Send,
{
    pub(in crate::scheduler) async fn clear_runtime_metadata(
        &mut self,
    ) -> Result<(), SchedulerError> {
        log_scheduler_debug(
            &self.logger,
            self.log_level,
            format_args!("Scheduler heatbeater shutting down"),
        );
        // Reference: Asynq v0.26.0 scheduler shutdown logs failures while
        // clearing scheduler entries and per-entry history, but does not
        // return them to the caller.
        // <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L329-L334>.
        if let Err(error) = self
            .broker
            .clear_scheduler_entries(&self.scheduler_id)
            .await
        {
            log_scheduler_error(
                &self.logger,
                self.log_level,
                format_args!("Failed to clear the scheduler entries: {error}"),
            );
        }
        for entry in &self.entries {
            if let Err(error) = self.broker.clear_scheduler_history(&entry.id).await {
                log_scheduler_warn(
                    &self.logger,
                    self.log_level,
                    format_args!(
                        "Could not clear scheduler history for entry {:?}: {error}",
                        entry.id
                    ),
                );
            }
        }
        Ok(())
    }
}
