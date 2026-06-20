use super::*;
use crate::aggregation::AggregatorRun;
use crate::server::worker::WorkerTaskRunner;
use crate::task::TaskMessage;
use crate::{BrokerError, DEFAULT_QUEUE_NAME, RedisRuntimeClient, Task, pb};
use prost::Message;
use std::future::Future;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

mod aggregation;
mod config;
mod fixtures;
mod health;
mod logging;
mod metadata;
mod queue;
mod runtime;
mod shutdown;
mod worker_runtime;

use fixtures::*;

trait ServerTestExt: Sized {
    fn with_maintenance_interval(self, duration: Duration) -> Self;
    fn with_recoverer_interval(self, duration: Duration) -> Self;
    fn with_syncer_interval(self, duration: Duration) -> Self;
    fn with_logger(self, logger: Arc<dyn Logger>) -> Self;
    fn with_health_check_handler<H>(self, handler: H) -> Self
    where
        H: HealthCheckHandler + 'static;
    fn with_server_metadata(self, metadata: ServerMetadata) -> Self;
    fn with_metadata_heartbeat_interval(self, duration: Duration) -> Self;
    fn with_default_server_metadata(self) -> Self;
    fn with_default_server_metadata_for_workers(
        self,
        worker_count: usize,
    ) -> Result<Self, ServerError>;
}

impl<P, S> ServerTestExt for Server<P, S> {
    fn with_maintenance_interval(mut self, duration: Duration) -> Self {
        self.forwarder_interval = normalize_interval(duration, DEFAULT_SERVER_FORWARDER_INTERVAL);
        self.recoverer_interval = normalize_interval(duration, DEFAULT_SERVER_RECOVERER_INTERVAL);
        self.janitor_interval = normalize_interval(duration, DEFAULT_SERVER_JANITOR_INTERVAL);
        self
    }

    fn with_recoverer_interval(mut self, duration: Duration) -> Self {
        self.recoverer_interval = normalize_interval(duration, DEFAULT_SERVER_RECOVERER_INTERVAL);
        self
    }

    fn with_syncer_interval(mut self, duration: Duration) -> Self {
        self.syncer_interval = normalize_interval(duration, DEFAULT_SERVER_SYNCER_INTERVAL);
        self
    }

    fn with_logger(self, logger: Arc<dyn Logger>) -> Self {
        test_support::with_logger(self, logger)
    }

    fn with_health_check_handler<H>(self, handler: H) -> Self
    where
        H: HealthCheckHandler + 'static,
    {
        test_support::with_health_check_handler(self, handler)
    }

    fn with_server_metadata(self, metadata: ServerMetadata) -> Self {
        test_support::with_server_metadata(self, metadata)
    }

    fn with_metadata_heartbeat_interval(self, duration: Duration) -> Self {
        test_support::with_metadata_heartbeat_interval(self, duration)
    }

    fn with_default_server_metadata(self) -> Self {
        test_support::with_default_server_metadata(self)
    }

    fn with_default_server_metadata_for_workers(
        self,
        worker_count: usize,
    ) -> Result<Self, ServerError> {
        test_support::with_default_server_metadata_for_workers(self, worker_count)
    }
}
