use std::sync::Arc;

use tokio::{
    sync::{mpsc, watch},
    task::{JoinHandle, JoinSet},
};

use crate::server::error::ServerError;
use crate::server::health::run_health_checks;
use crate::server::logging::{log_debug, log_info};
use crate::server::maintenance::{ServerMaintenanceIntervals, start_parallel_maintenance};
use crate::server::metadata::{clear_server_metadata_ignoring_error, run_metadata_heartbeat};
use crate::server::summary::ServerRunSummary;
use crate::server::syncer::start_syncer;
use crate::server::worker::{
    ServerClock, ServerConnection, ServerHeartbeatStore, ServerLeaseExtender,
    ServerMaintenanceRunner, ServerShutdown, ServerSyncStore, WorkerDrain, WorkerFactory,
    WorkerTaskRunner, run_worker_loop_with_stop,
};
use crate::server::{Server, Sleeper};

pub(in crate::server) struct ServerRuntime<P, S> {
    server: Server<P, S>,
    worker_count: usize,
    stop: watch::Receiver<bool>,
    shutdown: watch::Receiver<bool>,
    stopped: watch::Sender<bool>,
}

struct WorkerPool<W> {
    workers: JoinSet<WorkerDrain<W>>,
    stopped: JoinHandle<()>,
    shutdown_timeout: std::time::Duration,
}

impl<P, S> ServerRuntime<P, S>
where
    P: WorkerFactory
        + ServerConnection
        + ServerHeartbeatStore
        + ServerLeaseExtender
        + ServerMaintenanceRunner
        + ServerClock
        + ServerShutdown
        + ServerSyncStore
        + Clone
        + Send
        + 'static,
    S: Sleeper + Clone + Send + 'static,
{
    pub(in crate::server) fn new(
        server: Server<P, S>,
        worker_count: usize,
        stop: watch::Receiver<bool>,
        shutdown: watch::Receiver<bool>,
        stopped: watch::Sender<bool>,
    ) -> Self {
        Self {
            server,
            worker_count,
            stop,
            shutdown,
            stopped,
        }
    }

    /// Runs the server-owned parallel runtime.
    ///
    /// Reference: Asynq v0.26.0 `Server.Start` owns the processor workers and
    /// background components as one lifecycle:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L663-L721>.
    pub(in crate::server) async fn run(mut self) -> Result<ServerRunSummary, ServerError> {
        if self.worker_count == 0 {
            return Err(ServerError::EmptyWorkerCount);
        }

        log_info(
            &self.server.logger,
            self.server.log_level,
            "Starting processing",
        );
        let (runtime_shutdown_tx, runtime_shutdown_rx) = watch::channel(*self.shutdown.borrow());
        let (syncer_shutdown_tx, syncer_shutdown_rx) = watch::channel(false);
        let (metadata_shutdown_tx, metadata_shutdown_rx) = watch::channel(false);
        let mut external_shutdown = self.shutdown.clone();
        let mut forwarder_shutdown = self.shutdown.clone();
        let runtime_shutdown_bridge = {
            let runtime_shutdown_tx = runtime_shutdown_tx.clone();
            tokio::spawn(async move {
                while !*external_shutdown.borrow() {
                    if external_shutdown.changed().await.is_err() {
                        break;
                    }
                }
                let _ = runtime_shutdown_tx.send(true);
            })
        };
        let logger = self.server.logger.clone();
        let log_level = self.server.log_level;
        let metadata = self.server.metadata.clone();
        let runtime_state = self.server.runtime_state.clone();
        let metadata_heartbeat_interval = self.server.metadata_heartbeat_interval;
        let health_check_handler = self.server.health_check_handler.clone();
        let health_check_interval = self.server.health_check_interval;
        let queues = Arc::clone(&self.server.queues);
        let maintenance_intervals = ServerMaintenanceIntervals::new(
            self.server.forwarder_interval,
            self.server.recoverer_interval,
            self.server.janitor_interval,
        );
        let syncer_handle = start_syncer(
            self.server.runtime.clone(),
            runtime_state.clone(),
            self.server.syncer_interval,
            logger.clone(),
            log_level,
            syncer_shutdown_rx,
        );
        let aggregation_handle = self.server.aggregation_runner.take().map(|runner| {
            let (aggregation_shutdown_tx, aggregation_shutdown_rx) = watch::channel(false);
            (
                aggregation_shutdown_tx,
                runner.run_until_stopped(aggregation_shutdown_rx),
            )
        });
        let listener_handle = self.server.cancellation_listener.take().map(|listener| {
            let (listener_shutdown_tx, listener_shutdown_rx) = watch::channel(false);
            (
                listener_shutdown_tx,
                listener.run_until_stopped(listener_shutdown_rx),
            )
        });
        let mut metadata_runtime = self.server.runtime.clone();
        let metadata_handle = metadata.clone().map(|metadata| {
            let logger = logger.clone();
            tokio::spawn(async move {
                run_metadata_heartbeat(
                    &mut metadata_runtime,
                    runtime_state,
                    &metadata,
                    metadata_heartbeat_interval,
                    &logger,
                    log_level,
                    metadata_shutdown_rx,
                )
                .await
            })
        });
        let mut health_check_runtime = self.server.runtime.clone();
        let health_check_handle = health_check_handler.map(|handler| {
            let (health_check_shutdown_tx, health_check_shutdown_rx) = watch::channel(false);
            let logger = logger.clone();
            (
                health_check_shutdown_tx,
                tokio::spawn(async move {
                    run_health_checks(
                        &mut health_check_runtime,
                        handler,
                        health_check_interval,
                        &logger,
                        log_level,
                        health_check_shutdown_rx,
                    )
                    .await
                }),
            )
        });

        let mut maintenance_handles = start_parallel_maintenance(
            self.server.runtime.clone(),
            Arc::clone(&queues),
            maintenance_intervals,
            logger.clone(),
            log_level,
        );
        let mut worker_pool = WorkerPool::start(
            &self.server,
            self.worker_count,
            self.stop.clone(),
            runtime_shutdown_rx.clone(),
            self.stopped,
        );

        let mut summary = ServerRunSummary::default();
        let mut first_error = None;
        let mut graceful_shutdown_started = false;
        loop {
            if !graceful_shutdown_started && *self.shutdown.borrow() {
                graceful_shutdown_started = true;
                log_info(&logger, log_level, "Starting graceful shutdown");
                match maintenance_handles
                    .shutdown_forwarder(&logger, log_level)
                    .await
                {
                    Ok(run) => summary.record_maintenance(run),
                    Err(error) => {
                        if first_error.is_none() {
                            first_error = Some(error);
                            let _ = runtime_shutdown_tx.send(true);
                        }
                    }
                }
            }
            tokio::select! {
                result = worker_pool.join_next() => {
                    let Some(result) = result else {
                        break;
                    };
                    match result {
                        Ok(worker_summary) => summary.merge(worker_summary),
                        Err(error) => {
                            if first_error.is_none() {
                                first_error = Some(error);
                                let _ = runtime_shutdown_tx.send(true);
                            }
                        }
                    }
                }
                changed = forwarder_shutdown.changed(), if !graceful_shutdown_started => {
                    if changed.is_err() || *forwarder_shutdown.borrow() {
                        graceful_shutdown_started = true;
                        log_info(&logger, log_level, "Starting graceful shutdown");
                        match maintenance_handles
                            .shutdown_forwarder(&logger, log_level)
                            .await
                        {
                            Ok(run) => summary.record_maintenance(run),
                            Err(error) => {
                                if first_error.is_none() {
                                    first_error = Some(error);
                                    let _ = runtime_shutdown_tx.send(true);
                                }
                            }
                        }
                    }
                }
            };
        }
        worker_pool.wait_stopped().await;
        if !graceful_shutdown_started {
            log_info(&logger, log_level, "Starting graceful shutdown");
        }
        // Reference: Asynq v0.26.0 logs processor completion once the worker
        // pool has stopped accepting and draining active work:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L152-L160>.
        log_debug(&logger, log_level, "Worker runtime done");
        let _ = maintenance_handles
            .shutdown_forwarder(&logger, log_level)
            .await
            .map(|run| summary.record_maintenance(run))
            .map_err(|error| {
                if first_error.is_none() {
                    first_error = Some(error);
                    let _ = runtime_shutdown_tx.send(true);
                }
            });
        match maintenance_handles
            .shutdown_recoverer(&logger, log_level)
            .await
        {
            Ok(run) => summary.record_maintenance(run),
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                    let _ = runtime_shutdown_tx.send(true);
                }
            }
        }
        log_debug(&logger, log_level, "Syncer shutting down...");
        let _ = syncer_shutdown_tx.send(true);
        match syncer_handle.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                if first_error.is_none() {
                    first_error = Some(error);
                    let _ = runtime_shutdown_tx.send(true);
                }
            }
            Err(_) => {
                if first_error.is_none() {
                    first_error = Some(ServerError::WorkerThreadPanicked);
                    let _ = runtime_shutdown_tx.send(true);
                }
            }
        }
        if let Some((listener_shutdown_tx, handle)) = listener_handle {
            log_debug(&logger, log_level, "Subscriber shutting down...");
            let _ = listener_shutdown_tx.send(true);
            match handle.await {
                Ok(Ok(_)) => {
                    log_debug(&logger, log_level, "Subscriber done");
                }
                Ok(Err(error)) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
                Err(_) => {
                    if first_error.is_none() {
                        first_error = Some(ServerError::WorkerThreadPanicked);
                    }
                }
            }
        }
        match maintenance_handles
            .shutdown_janitor(&logger, log_level)
            .await
        {
            Ok(run) => summary.record_maintenance(run),
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                    let _ = runtime_shutdown_tx.send(true);
                }
            }
        }
        if let Some((aggregation_shutdown_tx, handle)) = aggregation_handle {
            log_debug(&logger, log_level, "Aggregator shutting down...");
            let _ = aggregation_shutdown_tx.send(true);
            match handle.await {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => {
                    if first_error.is_none() {
                        first_error = Some(ServerError::AggregationRunner(error.to_string()));
                    }
                }
                Err(_) => {
                    if first_error.is_none() {
                        first_error = Some(ServerError::WorkerThreadPanicked);
                    }
                }
            }
        }
        if let Some((health_check_shutdown_tx, handle)) = health_check_handle {
            log_debug(&logger, log_level, "Healthchecker shutting down...");
            let _ = health_check_shutdown_tx.send(true);
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
                Err(_) => {
                    if first_error.is_none() {
                        first_error = Some(ServerError::WorkerThreadPanicked);
                    }
                }
            }
        }
        if let Some(handle) = metadata_handle {
            let _ = metadata_shutdown_tx.send(true);
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
                Err(_) => {
                    if first_error.is_none() {
                        first_error = Some(ServerError::WorkerThreadPanicked);
                    }
                }
            }
        }
        if let Some(metadata) = metadata {
            let mut runtime = self.server.runtime.clone();
            clear_server_metadata_ignoring_error(&mut runtime, &metadata, &logger, log_level).await;
        }
        if !self.server.shared_connection {
            if let Err(error) = self.server.runtime.close() {
                first_error.get_or_insert(ServerError::Close(error));
            }
        }
        runtime_shutdown_bridge.abort();
        let _ = runtime_shutdown_bridge.await;
        log_info(&logger, log_level, "Exiting");
        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(summary)
        }
    }
}

impl<W> WorkerPool<W>
where
    W: ServerShutdown + WorkerTaskRunner + Send + 'static,
{
    /// Starts worker tasks owned by the server runtime.
    ///
    /// Reference: Asynq v0.26.0 processor workers share one server lifecycle
    /// and stop polling before shutdown-owned background components exit:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L723-L769>.
    fn start<P, S>(
        server: &Server<P, S>,
        worker_count: usize,
        stop: watch::Receiver<bool>,
        shutdown: watch::Receiver<bool>,
        stopped: watch::Sender<bool>,
    ) -> Self
    where
        P: WorkerFactory<WorkerHandle = W>,
        S: Sleeper + Clone + Send + 'static,
    {
        let (worker_stopped_tx, mut worker_stopped_rx) = mpsc::unbounded_channel();
        let stopped_handle = tokio::spawn(async move {
            for _ in 0..worker_count {
                if worker_stopped_rx.recv().await.is_none() {
                    return;
                }
            }
            let _ = stopped.send(true);
        });
        let mut workers = JoinSet::new();
        for _ in 0..worker_count {
            let worker = server.runtime.worker();
            let mut queue_selector = server.queue_selector.clone();
            let mut sleeper = server.sleeper.clone();
            let stop = stop.clone();
            let shutdown = shutdown.clone();
            let worker_stopped = worker_stopped_tx.clone();
            let idle_sleep = server.idle_sleep;
            let shutdown_timeout = server.shutdown_timeout;
            workers.spawn(async move {
                run_worker_loop_with_stop(
                    worker,
                    super::super::worker::WorkerLoopContext {
                        queue_selector: &mut queue_selector,
                        sleeper: &mut sleeper,
                        stop,
                        shutdown,
                        stopped: Some(worker_stopped),
                    },
                    super::super::worker::WorkerLoopTiming {
                        idle_sleep,
                        shutdown_timeout,
                    },
                )
                .await
            });
        }
        drop(worker_stopped_tx);
        Self {
            workers,
            stopped: stopped_handle,
            shutdown_timeout: server.shutdown_timeout,
        }
    }

    async fn join_next(&mut self) -> Option<Result<ServerRunSummary, ServerError>> {
        let result = self.workers.join_next().await?;
        Some(match result {
            Ok(mut drain) => {
                let summary = drain.summary().clone();
                let first_error = drain.error();
                let mut handle = drain.into_handle();
                let shutdown =
                    match tokio::time::timeout(self.shutdown_timeout, handle.shutdown()).await {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(error)) => Err(ServerError::from(error)),
                        Err(_) => Err(ServerError::ShutdownTimedOut),
                    };
                match (first_error, shutdown) {
                    (Some(error), _) => Err(error),
                    (None, Err(error)) => Err(error),
                    (None, Ok(())) => Ok(summary),
                }
            }
            Err(_) => Err(ServerError::WorkerThreadPanicked),
        })
    }

    async fn wait_stopped(self) {
        let _ = self.stopped.await;
    }
}
