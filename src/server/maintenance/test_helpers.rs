//! Test-only maintenance helpers for server-owned worker assemblies.
//!
//! Reference: Asynq v0.26.0 starts background forwarder, recoverer, and
//! janitor components from `Server.Start`:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L687-L695>.

use crate::aggregation::AggregationBroker;
use crate::processing::{IsFailure, ProcessingError, RetryDelay};
use crate::server::WorkerAssembly;
use crate::server::{
    ArchiveBroker, CleanupBroker, ForwardBroker, RecoverBroker, RetryBroker, ServerMaintenanceRun,
};

use super::assembly::{
    run_forwarder_maintenance, run_janitor_maintenance, run_recoverer_maintenance,
};

impl<B, H, R, C, I, E, L> WorkerAssembly<B, H, R, C, I, E, L>
where
    B: ForwardBroker
        + RecoverBroker
        + RetryBroker
        + ArchiveBroker
        + CleanupBroker
        + AggregationBroker
        + Send,
    R: RetryDelay + Send,
    C: crate::client::Clock + Send + Sync,
    I: IsFailure + Send,
{
    pub(crate) async fn run_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        let mut result = run_forwarder_maintenance(self, queues).await?;
        result.merge(run_recoverer_maintenance(self, queues).await?);
        result.merge(run_janitor_maintenance(self, queues).await?);
        Ok(result)
    }
}

impl<B, H, R, C, I, E, L> WorkerAssembly<B, H, R, C, I, E, L>
where
    B: CleanupBroker + Send,
    R: RetryDelay + Send,
    C: crate::client::Clock + Send + Sync,
    I: IsFailure + Send,
{
    pub(crate) async fn run_janitor_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        run_janitor_maintenance(self, queues).await
    }
}

impl<B, H, R, C, I, E, L> WorkerAssembly<B, H, R, C, I, E, L>
where
    B: RecoverBroker + RetryBroker + ArchiveBroker + AggregationBroker + Send,
    R: RetryDelay + Send,
    C: crate::client::Clock + Send + Sync,
    I: IsFailure + Send,
{
    pub(crate) async fn run_recoverer_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        run_recoverer_maintenance(self, queues).await
    }
}
