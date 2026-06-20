use crate::client::Clock;

use crate::scheduler::SchedulerError;
use crate::scheduler::broker::SchedulerBroker;
use crate::scheduler::core::Scheduler;
use crate::scheduler::logging::log_scheduler_warn;

impl<B, C> Scheduler<B, C>
where
    B: SchedulerBroker + Send,
    C: Clock + Send + Sync,
{
    async fn write_metadata(&mut self) -> Result<(), SchedulerError> {
        let entries = self
            .entries
            .iter()
            .map(|entry| (entry.id.clone(), entry.metadata()))
            .collect();
        self.broker
            .write_scheduler_entries(&self.scheduler_id, entries, self.metadata_ttl)
            .await?;
        Ok(())
    }

    pub(super) async fn write_metadata_ignoring_error(&mut self) {
        // Reference: Asynq v0.26.0 scheduler heartbeat logs
        // `WriteSchedulerEntries` failures and keeps the scheduler running:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L322-L346>.
        if let Err(error) = self.write_metadata().await {
            let error = match &error {
                SchedulerError::Metadata(error) => error.to_string(),
                error => error.to_string(),
            };
            log_scheduler_warn(
                &self.logger,
                self.log_level,
                format_args!("Scheduler could not write heartbeat data: {error}"),
            );
        }
    }
}
