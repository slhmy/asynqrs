use tokio::sync::oneshot;

use super::super::super::SchedulerError;
use super::super::{SchedulerCommand, SchedulerHandle};

impl SchedulerHandle {
    /// Removes a registered scheduler entry from a running background
    /// scheduler.
    ///
    /// Reference: Asynq v0.26.0 `Scheduler.Unregister` can remove entries
    /// after the scheduler has started:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L233-L244>.
    pub async fn unregister(&self, entry_id: &str) -> Result<(), SchedulerError> {
        let (result_tx, result_rx) = oneshot::channel();
        let command = SchedulerCommand::Unregister {
            entry_id: entry_id.to_owned(),
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
}
