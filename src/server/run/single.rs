use tokio::sync::watch;

use crate::server::error::ServerError;
use crate::server::logging::{log_debug, log_info};
use crate::server::maintenance::ServerMaintenanceIntervals;
use crate::server::metadata::clear_server_metadata_ignoring_error;
use crate::server::runtime::run_loop_with_stop;
use crate::server::summary::ServerRunSummary;
use crate::server::worker::{
    BorrowedWorkerFactory, ServerClock, ServerConnection, ServerHeartbeatStore,
    ServerLeaseExtender, ServerMaintenanceRunner, ServerShutdown, ServerSyncStore,
};
use crate::server::{Server, Sleeper};

impl<P, S> Server<P, S>
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
    pub async fn run(&mut self) -> Result<ServerRunSummary, ServerError> {
        let (stop, shutdown) = crate::signal::server_signal_receivers();
        self.run_until_stopped_with_stop(stop, shutdown).await
    }

    pub(crate) async fn run_until_stopped_with_stop(
        &mut self,
        mut stop: watch::Receiver<bool>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<ServerRunSummary, ServerError> {
        self.begin_start()?;
        log_info(&self.logger, self.log_level, "Starting processing");
        let aggregation_handle = self.aggregation_runner.as_ref().map(|runner| {
            let (aggregation_shutdown_tx, aggregation_shutdown_rx) = watch::channel(false);
            (
                aggregation_shutdown_tx,
                runner.run_until_stopped(aggregation_shutdown_rx),
            )
        });
        let listener_handle = self.cancellation_listener.as_ref().map(|listener| {
            let (listener_shutdown_tx, listener_shutdown_rx) = watch::channel(false);
            (
                listener_shutdown_tx,
                listener.run_until_stopped(listener_shutdown_rx),
            )
        });
        let result = run_loop_with_stop(
            &mut self.runtime,
            super::super::runtime::ServerLoopWork {
                queues: &self.queues,
                queue_selector: &mut self.queue_selector,
                sleeper: &mut self.sleeper,
                runtime_state: self.runtime_state.clone(),
            },
            super::super::runtime::ServerLoopTiming {
                idle_sleep: self.idle_sleep,
                maintenance_intervals: ServerMaintenanceIntervals::new(
                    self.forwarder_interval,
                    self.recoverer_interval,
                    self.janitor_interval,
                ),
                shutdown_timeout: self.shutdown_timeout,
                syncer_interval: self.syncer_interval,
                health_check_interval: self.health_check_interval,
                metadata_heartbeat_interval: self.metadata_heartbeat_interval,
            },
            super::super::runtime::ServerLoopObservability {
                health_check_handler: self.health_check_handler.clone(),
                metadata: self.metadata.as_ref(),
                logger: &self.logger,
                log_level: self.log_level,
            },
            super::super::runtime::ServerLoopSignals {
                stop: &mut stop,
                shutdown: &mut shutdown,
            },
        )
        .await;
        self.mark_closed();
        if let Some((listener_shutdown_tx, listener_handle)) = listener_handle {
            // Reference: Asynq v0.26.0 `Server.Shutdown` stops the
            // cancellation subscriber before the aggregation runner:
            // <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L738-L744>.
            log_debug(&self.logger, self.log_level, "Subscriber shutting down...");
            let _ = listener_shutdown_tx.send(true);
            listener_handle
                .await
                .map_err(|_| ServerError::WorkerThreadPanicked)??;
            log_debug(&self.logger, self.log_level, "Subscriber done");
        }
        if let Some((aggregation_shutdown_tx, aggregation_handle)) = aggregation_handle {
            log_debug(&self.logger, self.log_level, "Aggregator shutting down...");
            let _ = aggregation_shutdown_tx.send(true);
            aggregation_handle
                .await
                .map_err(|_| ServerError::WorkerThreadPanicked)?
                .map_err(|error| ServerError::AggregationRunner(error.to_string()))?;
        }
        if self.health_check_handler.is_some() {
            // Reference: Asynq v0.26.0 `Server.Shutdown` stops the
            // healthchecker after the aggregation runner:
            // <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L738-L744>.
            log_debug(
                &self.logger,
                self.log_level,
                "Healthchecker shutting down...",
            );
            log_debug(&self.logger, self.log_level, "Healthchecker done");
        }
        if let Some(metadata) = &self.metadata {
            // Reference: Asynq v0.26.0 `Server.Shutdown` stops the
            // heartbeater after healthchecker shutdown:
            // <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L738-L744>.
            clear_server_metadata_ignoring_error(
                &mut self.runtime,
                metadata,
                &self.logger,
                self.log_level,
            )
            .await;
        }
        if !self.shared_connection {
            // Reference: Asynq v0.26.0 `NewServer` owns its Redis client and
            // closes the broker on shutdown, while `NewServerFromRedisClient`
            // leaves caller-owned Redis clients open:
            // <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L723-L756>.
            self.runtime.close().map_err(ServerError::Close)?;
        }
        log_info(&self.logger, self.log_level, "Exiting");
        result
    }
}
