//! Server-owned lifecycle syncer loop.
//!
//! Reference: Asynq v0.26.0 starts a syncer alongside workers so failed
//! lifecycle writes are retried independently from task processing:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/syncer.go>.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use super::defaults::DEFAULT_SERVER_SYNCER_INTERVAL;
use super::logging::log_debug;
use super::{
    LogLevel, Logger, PendingSyncDrainPolicy, ServerClock, ServerError, ServerRuntimeState,
    ServerSyncStore, normalize_interval,
};

pub(in crate::server) fn start_syncer<P>(
    runtime: P,
    runtime_state: ServerRuntimeState,
    interval: Duration,
    logger: Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<Result<(), ServerError>>
where
    P: ServerClock + ServerSyncStore + Send + 'static,
{
    tokio::spawn(run_syncer(
        runtime,
        runtime_state,
        interval,
        logger,
        log_level,
        shutdown,
    ))
}

pub(in crate::server) async fn run_syncer<P>(
    mut runtime: P,
    runtime_state: ServerRuntimeState,
    interval: Duration,
    logger: Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ServerError>
where
    P: ServerClock + ServerSyncStore + Send,
{
    let mut ticker =
        tokio::time::interval(normalize_interval(interval, DEFAULT_SERVER_SYNCER_INTERVAL));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;

    while !*shutdown.borrow() {
        tokio::select! {
            _ = ticker.tick() => {
                let now = runtime.runtime_now();
                runtime_state
                    .drain_pending_sync(
                        &mut runtime,
                        PendingSyncDrainPolicy::Periodic,
                        &logger,
                        log_level,
                        now,
                    )
                    .await;
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
        }
    }
    // Reference: Asynq v0.26.0 syncer retries queued lifecycle writes one
    // final time after receiving shutdown:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/syncer.go#L53-L60>.
    let now = runtime.runtime_now();
    runtime_state
        .drain_pending_sync(
            &mut runtime,
            PendingSyncDrainPolicy::ShutdownFinal,
            &logger,
            log_level,
            now,
        )
        .await;
    // Reference: Asynq v0.26.0 background lifecycle components log a debug
    // `done` message after shutdown cleanup:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/syncer.go>.
    log_debug(&logger, log_level, "Syncer done");
    Ok(())
}
