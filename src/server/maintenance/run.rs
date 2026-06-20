use std::sync::Arc;

use super::ServerMaintenancePhase;
use super::logging::log_maintenance_error;
use crate::server::{LogLevel, Logger, ServerMaintenanceRun, ServerMaintenanceRunner};

pub(in crate::server) async fn run_maintenance_phase<P>(
    runtime: &mut P,
    queues: &[String],
    phase: ServerMaintenancePhase,
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
) -> ServerMaintenanceRun
where
    P: ServerMaintenanceRunner + Send,
{
    let result = match phase {
        ServerMaintenancePhase::Forwarder => runtime.run_forwarder_maintenance(queues).await,
        ServerMaintenancePhase::Recoverer => runtime.run_recoverer_maintenance(queues).await,
        ServerMaintenancePhase::Janitor => runtime.run_janitor_maintenance(queues).await,
    };
    match result {
        Ok(run) => run,
        Err(error) => {
            // Reference: Asynq v0.26.0 forwarder, recoverer, and janitor log
            // tick failures and keep future lifecycle ticks running:
            // <https://github.com/hibiken/asynq/blob/v0.26.0/forwarder.go#L61-L65>,
            // <https://github.com/hibiken/asynq/blob/v0.26.0/recoverer.go#L72-L94>,
            // <https://github.com/hibiken/asynq/blob/v0.26.0/janitor.go#L67-L74>.
            log_maintenance_error(logger, log_level, phase, &error);
            ServerMaintenanceRun::default()
        }
    }
}
