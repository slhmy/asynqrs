use super::super::SchedulerError;
use super::{SchedulerCommand, SchedulerHandle, SchedulerRun};

impl SchedulerHandle {
    /// Checks whether the scheduler backing store is reachable while running.
    ///
    /// Reference: Asynq v0.26.0 public `Scheduler.Ping` method delegates to
    /// the broker before shutdown:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L367-L375>.
    pub async fn ping(&self) -> Result<(), SchedulerError> {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        self.commands
            .send(SchedulerCommand::Ping { result: result_tx })
            .await
            .map_err(|_| SchedulerError::AlreadyStopped)?;
        result_rx
            .await
            .unwrap_or(Err(SchedulerError::AlreadyStopped))
    }

    /// Stops the running scheduler and waits for metadata cleanup.
    ///
    /// Reference: Asynq v0.26.0 `Scheduler.Shutdown` signals the active
    /// scheduler to stop and clear scheduler runtime metadata:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L309-L334>.
    pub async fn shutdown(self) -> Result<SchedulerRun, SchedulerError> {
        let _ = self.shutdown.send(true);
        self.wait().await
    }

    /// Waits until a separately stopped scheduler exits or fails.
    pub async fn wait(self) -> Result<SchedulerRun, SchedulerError> {
        self.run
            .await
            .unwrap_or_else(|error| Err(SchedulerError::BackgroundTask(error.to_string())))
    }
}
