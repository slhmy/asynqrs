use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::{
    sync::{Mutex, watch},
    task::JoinHandle,
};

use crate::server::error::ServerError;
use crate::server::logging::log_info;
use crate::server::summary::ServerRunSummary;
use crate::server::worker::{
    ServerClock, ServerConnection, ServerHeartbeatStore, ServerLeaseExtender,
    ServerMaintenanceRunner, ServerShutdown, ServerSyncStore, WorkerFactory,
};
use crate::server::{LogLevel, Logger, Server, Sleeper};

use super::runtime::ServerRuntime;

/// Handle returned by `Server::start` for a background server run.
///
/// Reference: Asynq v0.26.0 `Server.Start` starts processing without
/// blocking the caller, while `Server.Shutdown` later stops workers and
/// background components:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L663-L721>.
pub struct ServerHandle {
    stop: watch::Sender<bool>,
    shutdown: watch::Sender<bool>,
    stopped: watch::Receiver<bool>,
    run: JoinHandle<Result<ServerRunSummary, ServerError>>,
    pinger: Arc<dyn ServerHandlePinger>,
    logger: Option<Arc<dyn Logger>>,
    log_level: LogLevel,
}

#[async_trait]
trait ServerHandlePinger: Send + Sync {
    async fn ping(&self) -> Result<(), ServerError>;
}

struct ClonedServerPinger<P> {
    runtime: Mutex<P>,
}

#[async_trait]
impl<P> ServerHandlePinger for ClonedServerPinger<P>
where
    P: ServerConnection + Send,
{
    async fn ping(&self) -> Result<(), ServerError> {
        self.runtime
            .lock()
            .await
            .ping()
            .await
            .map_err(ServerError::Ping)
    }
}

impl fmt::Debug for ServerHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerHandle")
            .field("has_logger", &self.logger.is_some())
            .field("log_level", &self.log_level)
            .finish_non_exhaustive()
    }
}

impl ServerHandle {
    /// Stops processing new tasks and waits for worker loops to become idle.
    ///
    /// Reference: Asynq v0.26.0 public `Server.Stop` method stops the
    /// upstream processor before full server shutdown:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L758-L769>.
    ///
    /// Stop ownership is coordinated by the server-owned worker pool; full
    /// worker runtime shutdown remains tied to `Shutdown`.
    pub async fn stop(&self) -> Result<(), ServerError> {
        if *self.stopped.borrow() {
            return Ok(());
        }
        if !*self.stop.borrow() {
            log_info(&self.logger, self.log_level, "Stopping worker runtime");
            let _ = self.stop.send(true);
        }
        let mut stopped = self.stopped.clone();
        while !*stopped.borrow() {
            if stopped.changed().await.is_err() {
                break;
            }
        }
        if *stopped.borrow() {
            log_info(&self.logger, self.log_level, "Worker runtime stopped");
        }
        Ok(())
    }

    /// Returns true once the server has stopped polling for new tasks.
    pub fn is_stopped(&self) -> bool {
        *self.stopped.borrow()
    }

    /// Checks whether the backing broker is reachable while the server runs.
    ///
    /// Reference: Asynq v0.26.0 public `Server.Ping` method delegates to the
    /// broker even while the server lifecycle is managed separately:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L776-L786>.
    pub async fn ping(&self) -> Result<(), ServerError> {
        self.pinger.ping().await
    }

    /// Stops the running server and waits for graceful shutdown to finish.
    ///
    /// Reference: Asynq v0.26.0 public `Server.Shutdown` method signals the
    /// running worker, syncer, healthchecker, subscriber, recoverer, and
    /// forwarder components:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L723-L756>.
    pub async fn shutdown(self) -> Result<ServerRunSummary, ServerError> {
        let _ = self.stop.send(true);
        let _ = self.shutdown.send(true);
        self.wait().await
    }

    /// Waits until a separately stopped server exits or fails.
    pub async fn wait(self) -> Result<ServerRunSummary, ServerError> {
        self.run
            .await
            .map_err(|_| ServerError::WorkerThreadPanicked)?
    }
}

impl<P, S> Server<P, S>
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
    /// Starts the server in a background task and returns immediately.
    ///
    /// Reference: Asynq v0.26.0 public `Server.Start` method starts workers
    /// using the server's configured concurrency without blocking the caller:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L663-L721>.
    ///
    /// Known upstream gap: `Start` does not own upstream-style handler
    /// arguments or OS-signal coordination yet.
    pub fn start(mut self) -> Result<ServerHandle, ServerError> {
        self.begin_start()?;
        if self.worker_count == 0 {
            return Err(ServerError::EmptyWorkerCount);
        }

        let logger = self.logger.clone();
        let log_level = self.log_level;
        let pinger = Arc::new(ClonedServerPinger {
            runtime: Mutex::new(self.runtime.clone()),
        });
        let (stop_tx, stop_rx) = watch::channel(false);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (stopped_tx, stopped_rx) = watch::channel(false);
        let run = tokio::spawn(async move {
            self.run_started_configured_parallel_with_stop(stop_rx, shutdown_rx, stopped_tx)
                .await
        });
        Ok(ServerHandle {
            stop: stop_tx,
            shutdown: shutdown_tx,
            stopped: stopped_rx,
            run,
            pinger,
            logger,
            log_level,
        })
    }

    async fn run_started_configured_parallel_with_stop(
        self,
        stop: watch::Receiver<bool>,
        shutdown: watch::Receiver<bool>,
        stopped: watch::Sender<bool>,
    ) -> Result<ServerRunSummary, ServerError> {
        let worker_count = self.worker_count;
        self.run_started_parallel_with_stop(worker_count, stop, shutdown, stopped)
            .await
    }

    async fn run_started_parallel_with_stop(
        self,
        worker_count: usize,
        stop: watch::Receiver<bool>,
        shutdown: watch::Receiver<bool>,
        stopped: watch::Sender<bool>,
    ) -> Result<ServerRunSummary, ServerError> {
        ServerRuntime::new(self, worker_count, stop, shutdown, stopped)
            .run()
            .await
    }
}
