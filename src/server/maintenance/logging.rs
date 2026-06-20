use std::sync::Arc;

use super::ServerMaintenancePhase;
use crate::ProcessingError;
use crate::server::logging::{log_debug, log_error, log_warn};
use crate::server::{LogLevel, Logger};

pub(in crate::server) fn log_maintenance_error(
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    phase: ServerMaintenancePhase,
    error: &ProcessingError,
) {
    let error = processing_error_source(error);
    match phase {
        ServerMaintenancePhase::Forwarder => log_error(
            logger,
            log_level,
            format_args!("Failed to forward scheduled tasks: {error}"),
        ),
        ServerMaintenancePhase::Recoverer => log_warn(
            logger,
            log_level,
            format_args!("recoverer: could not list lease expired tasks: {error}"),
        ),
        ServerMaintenancePhase::Janitor => log_error(
            logger,
            log_level,
            format_args!("Failed to run janitor maintenance: {error}"),
        ),
    }
}

fn processing_error_source(error: &ProcessingError) -> String {
    match error {
        ProcessingError::Forward(error) => error.to_string(),
        ProcessingError::Recover(error) => error.to_string(),
        ProcessingError::Cleanup(error) => error.to_string(),
        error => error.to_string(),
    }
}

pub(in crate::server) fn log_maintenance_done(
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    phase: ServerMaintenancePhase,
) {
    // Reference: Asynq v0.26.0 forwarder, recoverer, and janitor goroutines
    // log a debug `done` message when their shutdown signal is received:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/forwarder.go#L43-L56>,
    // <https://github.com/hibiken/asynq/blob/v0.26.0/recoverer.go#L53-L67>,
    // <https://github.com/hibiken/asynq/blob/v0.26.0/janitor.go#L47-L61>.
    let message = match phase {
        ServerMaintenancePhase::Forwarder => "Forwarder done",
        ServerMaintenancePhase::Recoverer => "Recoverer done",
        ServerMaintenancePhase::Janitor => "Janitor done",
    };
    log_debug(logger, log_level, message);
}

pub(in crate::server) fn log_maintenance_shutdown(
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    phase: ServerMaintenancePhase,
) {
    // Reference: Asynq v0.26.0 `Server.Shutdown` asks each maintenance
    // component to shut down in component order:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L723-L756>.
    let message = match phase {
        ServerMaintenancePhase::Forwarder => "Forwarder shutting down...",
        ServerMaintenancePhase::Recoverer => "Recoverer shutting down...",
        ServerMaintenancePhase::Janitor => "Janitor shutting down...",
    };
    log_debug(logger, log_level, message);
}
