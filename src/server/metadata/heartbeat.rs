use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use super::ServerMetadata;
use crate::server::{
    DEFAULT_SERVER_METADATA_HEARTBEAT_INTERVAL, LogLevel, Logger, ServerClock, ServerError,
    ServerHeartbeatStore, ServerLeaseExtender,
    logging::{log_debug, log_error},
    runtime_state::ServerRuntimeState,
};

pub(in crate::server) async fn write_server_metadata_ignoring_error<P>(
    store: &mut P,
    metadata: &ServerMetadata,
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
) where
    P: ServerHeartbeatStore + Send,
{
    // Reference: Asynq v0.26.0 heartbeater logs `WriteServerState` failures
    // and keeps the server running:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/heartbeat.go#L143-L154>.
    if let Err(error) = store.write_server_metadata(metadata).await {
        log_error(
            logger,
            log_level,
            format_args!("Failed to write server state data: {error}"),
        );
    }
}

pub(in crate::server) async fn extend_active_worker_leases<P>(
    lease_extender: &mut P,
    runtime_state: &ServerRuntimeState,
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
) where
    P: ServerLeaseExtender + ServerClock + Send,
{
    // Reference: Asynq v0.26.0 heartbeater extends leases for currently
    // active workers on each heartbeat before the recoverer can reclaim them:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/heartbeat.go#L170-L201>.
    let now = lease_extender.runtime_now();
    runtime_state
        .extend_active_worker_leases(lease_extender, now, |task_ids, error| {
            log_error(
                logger,
                log_level,
                format_args!("Failed to extend lease for tasks {task_ids:?}: {error}"),
            );
        })
        .await;
}

pub(in crate::server) async fn clear_server_metadata_ignoring_error<P>(
    store: &mut P,
    metadata: &ServerMetadata,
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
) where
    P: ServerHeartbeatStore + Send,
{
    // Reference: Asynq v0.26.0 heartbeater shutdown logs before signalling
    // the heartbeater goroutine, then logs `Heartbeater done` after clearing
    // server state:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/heartbeat.go#L82-L100>.
    log_debug(logger, log_level, "Heartbeater shutting down...");
    // Reference: Asynq v0.26.0 heartbeater shutdown logs `ClearServerState`
    // failures and still lets server shutdown complete:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/heartbeat.go#L88-L100>.
    if let Err(error) = store.clear_server_metadata(metadata).await {
        log_error(
            logger,
            log_level,
            format_args!("Failed to clear server state: {error}"),
        );
    }
    log_debug(logger, log_level, "Heartbeater done");
}

pub(in crate::server) async fn run_metadata_heartbeat<P>(
    runtime: &mut P,
    runtime_state: ServerRuntimeState,
    metadata: &ServerMetadata,
    metadata_heartbeat_interval: Duration,
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ServerError>
where
    P: ServerHeartbeatStore + ServerLeaseExtender + ServerClock + Send,
{
    let metadata_heartbeat_interval =
        effective_metadata_heartbeat_interval(metadata_heartbeat_interval);
    // Reference: Asynq v0.26.0 heartbeater writes server state with a TTL
    // equal to twice the heartbeat interval:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/heartbeat.go#L143-L154>.
    let metadata = metadata.with_ttl(metadata_heartbeat_ttl(metadata_heartbeat_interval));
    let mut heartbeat = tokio::time::interval(metadata_heartbeat_interval);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await;

    extend_active_worker_leases(runtime, &runtime_state, logger, log_level).await;
    let snapshot = runtime_state.server_metadata_snapshot(&metadata);
    write_server_metadata_ignoring_error(runtime, &snapshot, logger, log_level).await;
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                extend_active_worker_leases(runtime, &runtime_state, logger, log_level).await;
                let snapshot = runtime_state.server_metadata_snapshot(&metadata);
                write_server_metadata_ignoring_error(runtime, &snapshot, logger, log_level).await;
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
        }
    }
    Ok(())
}

pub(in crate::server) fn effective_metadata_heartbeat_interval(interval: Duration) -> Duration {
    if interval.is_zero() {
        DEFAULT_SERVER_METADATA_HEARTBEAT_INTERVAL
    } else {
        interval
    }
}

pub(in crate::server) fn metadata_heartbeat_ttl(interval: Duration) -> Duration {
    // Reference: Asynq v0.26.0 heartbeater writes server state with a TTL
    // equal to twice the heartbeat interval:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/heartbeat.go#L143-L154>.
    interval.saturating_mul(2)
}
