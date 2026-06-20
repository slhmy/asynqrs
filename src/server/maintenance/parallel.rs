use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use super::logging::{log_maintenance_done, log_maintenance_shutdown};
use super::tickers::maintenance_ticker;
use super::{ServerMaintenanceIntervals, ServerMaintenancePhase, run_maintenance_phase};
use crate::server::{LogLevel, Logger, ServerError, ServerMaintenanceRun, ServerMaintenanceRunner};

pub(in crate::server) struct ParallelMaintenanceHandles {
    forwarder: Option<ParallelMaintenanceHandle>,
    recoverer: Option<ParallelMaintenanceHandle>,
    janitor: Option<ParallelMaintenanceHandle>,
}

struct ParallelMaintenanceHandle {
    phase: ServerMaintenancePhase,
    shutdown: watch::Sender<bool>,
    run: tokio::task::JoinHandle<Result<ServerMaintenanceRun, ServerError>>,
}

impl ParallelMaintenanceHandles {
    pub(in crate::server) async fn shutdown_forwarder(
        &mut self,
        logger: &Option<Arc<dyn Logger>>,
        log_level: LogLevel,
    ) -> Result<ServerMaintenanceRun, ServerError> {
        shutdown_maintenance_handle(&mut self.forwarder, logger, log_level).await
    }

    pub(in crate::server) async fn shutdown_recoverer(
        &mut self,
        logger: &Option<Arc<dyn Logger>>,
        log_level: LogLevel,
    ) -> Result<ServerMaintenanceRun, ServerError> {
        shutdown_maintenance_handle(&mut self.recoverer, logger, log_level).await
    }

    pub(in crate::server) async fn shutdown_janitor(
        &mut self,
        logger: &Option<Arc<dyn Logger>>,
        log_level: LogLevel,
    ) -> Result<ServerMaintenanceRun, ServerError> {
        shutdown_maintenance_handle(&mut self.janitor, logger, log_level).await
    }
}

pub(in crate::server) fn start_parallel_maintenance<P>(
    runtime: P,
    queues: Arc<[String]>,
    intervals: ServerMaintenanceIntervals,
    logger: Option<Arc<dyn Logger>>,
    log_level: LogLevel,
) -> ParallelMaintenanceHandles
where
    P: ServerMaintenanceRunner + Clone + Send + 'static,
{
    let (forwarder_shutdown_tx, forwarder_shutdown_rx) = watch::channel(false);
    let (recoverer_shutdown_tx, recoverer_shutdown_rx) = watch::channel(false);
    let (janitor_shutdown_tx, janitor_shutdown_rx) = watch::channel(false);
    ParallelMaintenanceHandles {
        forwarder: Some(ParallelMaintenanceHandle {
            phase: ServerMaintenancePhase::Forwarder,
            shutdown: forwarder_shutdown_tx,
            run: tokio::spawn(run_parallel_maintenance_phase(
                runtime.clone(),
                ParallelMaintenancePhaseContext {
                    queues: Arc::clone(&queues),
                    interval: intervals.forwarder,
                    phase: ServerMaintenancePhase::Forwarder,
                    run_immediately: false,
                    logger: logger.clone(),
                    log_level,
                    shutdown: forwarder_shutdown_rx,
                },
            )),
        }),
        recoverer: Some(ParallelMaintenanceHandle {
            phase: ServerMaintenancePhase::Recoverer,
            shutdown: recoverer_shutdown_tx,
            run: tokio::spawn(run_parallel_maintenance_phase(
                runtime.clone(),
                ParallelMaintenancePhaseContext {
                    queues: Arc::clone(&queues),
                    interval: intervals.recoverer,
                    phase: ServerMaintenancePhase::Recoverer,
                    run_immediately: true,
                    logger: logger.clone(),
                    log_level,
                    shutdown: recoverer_shutdown_rx,
                },
            )),
        }),
        janitor: Some(ParallelMaintenanceHandle {
            phase: ServerMaintenancePhase::Janitor,
            shutdown: janitor_shutdown_tx,
            run: tokio::spawn(run_parallel_maintenance_phase(
                runtime,
                ParallelMaintenancePhaseContext {
                    queues,
                    interval: intervals.janitor,
                    phase: ServerMaintenancePhase::Janitor,
                    run_immediately: false,
                    logger,
                    log_level,
                    shutdown: janitor_shutdown_rx,
                },
            )),
        }),
    }
}

async fn shutdown_maintenance_handle(
    handle: &mut Option<ParallelMaintenanceHandle>,
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
) -> Result<ServerMaintenanceRun, ServerError> {
    if let Some(handle) = handle.take() {
        log_maintenance_shutdown(logger, log_level, handle.phase);
        let _ = handle.shutdown.send(true);
        return handle
            .run
            .await
            .map_err(|_| ServerError::WorkerThreadPanicked)?;
    }
    Ok(ServerMaintenanceRun::default())
}

struct ParallelMaintenancePhaseContext {
    queues: Arc<[String]>,
    interval: Duration,
    phase: ServerMaintenancePhase,
    run_immediately: bool,
    logger: Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    shutdown: watch::Receiver<bool>,
}

async fn run_parallel_maintenance_phase<P>(
    mut runtime: P,
    context: ParallelMaintenancePhaseContext,
) -> Result<ServerMaintenanceRun, ServerError>
where
    P: ServerMaintenanceRunner + Send,
{
    let ParallelMaintenancePhaseContext {
        queues,
        interval,
        phase,
        run_immediately,
        logger,
        log_level,
        mut shutdown,
    } = context;
    let mut summary = ServerMaintenanceRun::default();
    let mut ticker = maintenance_ticker(interval);
    ticker.tick().await;
    if run_immediately && !*shutdown.borrow() {
        summary
            .merge(run_maintenance_phase(&mut runtime, &queues, phase, &logger, log_level).await);
    }

    while !*shutdown.borrow() {
        tokio::select! {
            _ = ticker.tick() => {
                summary.merge(run_maintenance_phase(&mut runtime, &queues, phase, &logger, log_level).await);
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
        }
    }
    log_maintenance_done(&logger, log_level, phase);
    Ok(summary)
}
