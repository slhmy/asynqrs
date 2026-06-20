//! Server runtime orchestration helpers.
//!
//! Reference: Asynq v0.26.0 `Server.Run` / `Server.Start` coordinate worker
//! processing with maintenance, heartbeat, healthcheck, and shutdown
//! lifecycle components:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L663-L721>.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use super::health::{effective_health_check_interval, run_health_check};
use super::logging::{log_debug, log_info};
use super::maintenance::{
    ServerMaintenanceIntervals, ServerMaintenancePhase, ServerMaintenanceTickers,
    run_maintenance_phase,
};
use super::metadata::{
    effective_metadata_heartbeat_interval, extend_active_worker_leases, metadata_heartbeat_ttl,
    write_server_metadata_ignoring_error,
};
use super::{
    BorrowedWorkerFactory, HealthCheckHandler, LogLevel, Logger, PendingSyncDrainPolicy,
    QueueSelector, ServerClock, ServerConnection, ServerError, ServerHeartbeatStore,
    ServerLeaseExtender, ServerMaintenanceRunner, ServerMetadata, ServerRunSummary,
    ServerRuntimeState, ServerShutdown, ServerSyncStore, Sleeper, WorkerRun,
};

pub(in crate::server) fn idle_poll_sleep_duration(task_check_interval: Duration) -> Duration {
    // Reference: Asynq v0.26.0 sleeps for `TaskCheckInterval/2` plus a
    // random jitter shorter than `TaskCheckInterval` after an empty dequeue
    // poll to avoid synchronized Redis polling:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L158-L164>.
    if task_check_interval.is_zero() {
        return Duration::ZERO;
    }
    let jitter_bound = task_check_interval.as_nanos().min(u128::from(u64::MAX)) as u64;
    let jitter = if jitter_bound == 0 {
        Duration::ZERO
    } else {
        Duration::from_nanos(rand::random_range(0..jitter_bound))
    };
    (task_check_interval / 2).saturating_add(jitter)
}

pub(super) struct ServerLoopWork<'a, S> {
    pub(super) queues: &'a [String],
    pub(super) queue_selector: &'a mut QueueSelector,
    pub(super) sleeper: &'a mut S,
    pub(super) runtime_state: ServerRuntimeState,
}

pub(super) struct ServerLoopTiming {
    pub(super) idle_sleep: Duration,
    pub(super) maintenance_intervals: ServerMaintenanceIntervals,
    pub(super) shutdown_timeout: Duration,
    pub(super) syncer_interval: Duration,
    pub(super) health_check_interval: Duration,
    pub(super) metadata_heartbeat_interval: Duration,
}

pub(super) struct ServerLoopObservability<'a> {
    pub(super) health_check_handler: Option<Arc<dyn HealthCheckHandler>>,
    pub(super) metadata: Option<&'a ServerMetadata>,
    pub(super) logger: &'a Option<Arc<dyn Logger>>,
    pub(super) log_level: LogLevel,
}

pub(super) struct ServerLoopSignals<'a> {
    pub(super) stop: &'a mut watch::Receiver<bool>,
    pub(super) shutdown: &'a mut watch::Receiver<bool>,
}

pub(super) async fn run_loop_with_stop<P, S>(
    runtime: &mut P,
    work: ServerLoopWork<'_, S>,
    timing: ServerLoopTiming,
    observability: ServerLoopObservability<'_>,
    signals: ServerLoopSignals<'_>,
) -> Result<ServerRunSummary, ServerError>
where
    P: BorrowedWorkerFactory
        + ServerConnection
        + ServerHeartbeatStore
        + ServerLeaseExtender
        + ServerMaintenanceRunner
        + ServerClock
        + ServerShutdown
        + ServerSyncStore
        + Send,
    S: Sleeper + Send,
{
    let ServerLoopWork {
        queues,
        queue_selector,
        sleeper,
        runtime_state,
    } = work;
    let ServerLoopTiming {
        idle_sleep,
        maintenance_intervals,
        shutdown_timeout,
        syncer_interval,
        health_check_interval,
        metadata_heartbeat_interval,
    } = timing;
    let ServerLoopObservability {
        health_check_handler,
        metadata,
        logger,
        log_level,
    } = observability;
    let ServerLoopSignals { stop, shutdown } = signals;
    let mut summary = ServerRunSummary::default();
    let mut first_error = None;
    let mut worker_timed_out = false;
    let metadata_heartbeat_interval =
        effective_metadata_heartbeat_interval(metadata_heartbeat_interval);
    let metadata_for_write = metadata
        .map(|metadata| metadata.with_ttl(metadata_heartbeat_ttl(metadata_heartbeat_interval)));
    let mut metadata_heartbeat = metadata.map(|_| {
        let mut interval = tokio::time::interval(metadata_heartbeat_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval
    });
    if let Some(metadata_heartbeat) = &mut metadata_heartbeat {
        metadata_heartbeat.tick().await;
    }
    if let Some(metadata) = &metadata_for_write {
        extend_active_worker_leases(runtime, &runtime_state, logger, log_level).await;
        let metadata = runtime_state.server_metadata_snapshot(metadata);
        write_server_metadata_ignoring_error(runtime, &metadata, logger, log_level).await;
    }
    let mut health_check = health_check_handler.as_ref().map(|_| {
        let mut interval =
            tokio::time::interval(effective_health_check_interval(health_check_interval));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval
    });
    if let Some(health_check) = &mut health_check {
        health_check.tick().await;
    }
    let mut maintenance_tickers = ServerMaintenanceTickers::new(maintenance_intervals);
    maintenance_tickers.skip_initial_ticks().await;
    let mut syncer = tokio::time::interval(syncer_interval);
    syncer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    syncer.tick().await;
    summary.record_maintenance(
        run_maintenance_phase(
            runtime,
            queues,
            ServerMaintenancePhase::Recoverer,
            logger,
            log_level,
        )
        .await,
    );
    while !*shutdown.borrow() {
        let selected_queues = queue_selector.select();
        let run = tokio::select! {
            phase = maintenance_tickers.next_phase() => {
                summary.record_maintenance(run_maintenance_phase(runtime, queues, phase, logger, log_level).await);
                continue;
            }
            _ = syncer.tick() => {
                // Reference: Asynq v0.26.0 runs a syncer alongside the
                // processor to retry failed lifecycle writes:
                // <https://github.com/hibiken/asynq/blob/v0.26.0/syncer.go>.
                let now = runtime.runtime_now();
                runtime_state
                    .drain_pending_sync(
                        runtime,
                        PendingSyncDrainPolicy::Periodic,
                        logger,
                        log_level,
                        now,
                    )
                    .await;
                continue;
            }
            _ = async {
                if let Some(metadata_heartbeat) = &mut metadata_heartbeat {
                    metadata_heartbeat.tick().await;
                }
            }, if metadata_heartbeat.is_some() => {
                extend_active_worker_leases(runtime, &runtime_state, logger, log_level).await;
                let metadata = runtime_state.server_metadata_snapshot(metadata_for_write.as_ref().expect("metadata guard checked"));
                write_server_metadata_ignoring_error(runtime, &metadata, logger, log_level).await;
                continue;
            }
            _ = async {
                if let Some(health_check) = &mut health_check {
                    health_check.tick().await;
                }
            }, if health_check.is_some() => {
                run_health_check(runtime, health_check_handler.as_ref().expect("health check guard checked")).await;
                continue;
            }
            // Reference: Asynq v0.26.0 `Server.Stop` stops the processor
            // from taking new tasks while shutdown-owned components keep
            // running until `Server.Shutdown`:
            // <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L758-L769>.
            run = run_once_until_shutdown(runtime, &selected_queues, shutdown.clone(), shutdown_timeout), if !*stop.borrow() => match run {
                Ok(RunOnceUntilShutdown::Finished(run)) => run,
                Ok(RunOnceUntilShutdown::TimedOut) => {
                    worker_timed_out = true;
                    break;
                }
                Ok(RunOnceUntilShutdown::Interrupted) => break,
                Err(error) => {
                    first_error = Some(error);
                    break;
                }
            },
            changed = stop.changed() => {
                if changed.is_err() {
                    break;
                }
                continue;
            }
            changed = shutdown.changed(), if *stop.borrow() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
                continue;
            }
        };
        match run {
            WorkerRun::NoProcessableTask => {
                summary.record_idle_poll();
                tokio::select! {
                    phase = maintenance_tickers.next_phase() => {
                        summary.record_maintenance(run_maintenance_phase(runtime, queues, phase, logger, log_level).await);
                    }
                    _ = syncer.tick() => {
                        let now = runtime.runtime_now();
                        runtime_state
                            .drain_pending_sync(
                        runtime,
                                PendingSyncDrainPolicy::Periodic,
                                logger,
                                log_level,
                                now,
                            )
                            .await;
                    }
                    _ = async {
                        if let Some(metadata_heartbeat) = &mut metadata_heartbeat {
                            metadata_heartbeat.tick().await;
                        }
                    }, if metadata_heartbeat.is_some() => {
                        extend_active_worker_leases(runtime, &runtime_state, logger, log_level).await;
                        let metadata = runtime_state.server_metadata_snapshot(metadata_for_write.as_ref().expect("metadata guard checked"));
                        write_server_metadata_ignoring_error(runtime, &metadata, logger, log_level).await;
                    }
                    _ = async {
                        if let Some(health_check) = &mut health_check {
                            health_check.tick().await;
                        }
                    }, if health_check.is_some() => {
                        run_health_check(runtime, health_check_handler.as_ref().expect("health check guard checked")).await;
                    }
                    _ = sleeper.sleep(idle_poll_sleep_duration(idle_sleep)) => {}
                    changed = stop.changed() => {
                        if changed.is_err() {
                            break;
                        }
                    }
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                    }
                }
            }
            result => summary.record(result),
        }
    }
    // Reference: Asynq v0.26.0 `Server.Shutdown` announces graceful shutdown
    // before reporting component shutdown completion:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L723-L756>.
    log_info(logger, log_level, "Starting graceful shutdown");
    // Reference: Asynq v0.26.0 logs when the processor goroutine exits after
    // receiving its done signal:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L152-L160>.
    log_debug(logger, log_level, "Worker runtime done");
    // Reference: Asynq v0.26.0 syncer retries queued lifecycle writes one
    // final time after receiving shutdown:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/syncer.go#L53-L60>.
    log_debug(logger, log_level, "Syncer shutting down...");
    let now = runtime.runtime_now();
    runtime_state
        .drain_pending_sync(
            runtime,
            PendingSyncDrainPolicy::ShutdownFinal,
            logger,
            log_level,
            now,
        )
        .await;
    // Reference: Asynq v0.26.0 syncer logs a debug `done` message after
    // shutdown cleanup:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/syncer.go>.
    log_debug(logger, log_level, "Syncer done");
    if worker_timed_out {
        // The borrowed worker handle already performed active task shutdown
        // before releasing the server-owned runtime borrow.
    } else {
        tokio::time::timeout(shutdown_timeout, runtime.shutdown())
            .await
            .map_err(|_| ServerError::ShutdownTimedOut)??;
    }
    if let Some(error) = first_error {
        Err(error)
    } else {
        Ok(summary)
    }
}

enum RunOnceUntilShutdown {
    Finished(WorkerRun),
    TimedOut,
    Interrupted,
}

enum RunOnceWait {
    Finished(Result<WorkerRun, ServerError>),
    TimedOut,
    Interrupted,
}

async fn run_once_until_shutdown<P>(
    runtime: &mut P,
    selected_queues: &[String],
    mut shutdown: watch::Receiver<bool>,
    shutdown_timeout: Duration,
) -> Result<RunOnceUntilShutdown, ServerError>
where
    P: BorrowedWorkerFactory + Send,
{
    let mut worker = runtime.borrowed_worker();
    let wait = {
        let run_once = worker.run_once(selected_queues);
        tokio::pin!(run_once);
        tokio::select! {
            run = &mut run_once => RunOnceWait::Finished(run.map_err(ServerError::from)),
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    // Reference: Asynq v0.26.0 `Server.Shutdown` waits up to
                    // `Config.ShutdownTimeout` for active workers before aborting
                    // and handing unfinished work to the shutdown/requeue path:
                    // <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L723-L756>.
                    match tokio::time::timeout(shutdown_timeout, &mut run_once).await {
                        Ok(run) => RunOnceWait::Finished(run.map_err(ServerError::from)),
                        Err(_) => RunOnceWait::TimedOut,
                    }
                } else {
                    RunOnceWait::Interrupted
                }
            }
        }
    };
    match wait {
        RunOnceWait::Finished(Ok(run)) => Ok(RunOnceUntilShutdown::Finished(run)),
        RunOnceWait::Finished(Err(error)) => Err(error),
        RunOnceWait::Interrupted => Ok(RunOnceUntilShutdown::Interrupted),
        RunOnceWait::TimedOut => {
            tokio::time::timeout(shutdown_timeout, worker.handle_mut().shutdown())
                .await
                .map_err(|_| ServerError::ShutdownTimedOut)??;
            Ok(RunOnceUntilShutdown::TimedOut)
        }
    }
}
