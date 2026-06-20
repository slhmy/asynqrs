use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::watch;

use crate::aggregation::{AggregationBroker, Aggregator, AggregatorRun};
use crate::{
    AggregationError, AggregationGroupConfig, AggregationHandler, GroupAggregator,
    server::{DEFAULT_SERVER_GROUP_GRACE_PERIOD, ServerError, Sleeper},
};

/// Shared holder for a `Config.GroupAggregator` callback.
///
/// Reference: Asynq v0.26.0 public `Config.GroupAggregator` field:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L239-L245>.
///
/// Rust runtime note: Redis-backed server construction applies this with the
/// Redis aggregation broker; generic server assembly remains crate-internal.
pub type SharedGroupAggregator = Arc<tokio::sync::Mutex<dyn GroupAggregator + Send>>;

#[async_trait]
impl GroupAggregator for SharedGroupAggregator {
    async fn aggregate(
        &mut self,
        group: &str,
        tasks: Vec<crate::Task>,
    ) -> Result<crate::Task, AggregationError> {
        self.lock().await.aggregate(group, tasks).await
    }
}

/// Background aggregation runner owned by `Server`.
///
/// Reference: Asynq v0.26.0 `Server.Start` launches the aggregator alongside
/// workers and other lifecycle components when group aggregation is configured:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L687-L695>.
pub(crate) trait AggregationRunner: Send + Sync + std::fmt::Debug {
    fn run_until_stopped(
        &self,
        shutdown: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<Result<AggregatorRun, ServerError>>;
}

/// `AggregationRunner` adapter for the crate's `Aggregator` runtime.
///
/// Reference: Asynq v0.26.0 server owns the aggregator lifecycle:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L215-L238>.
pub(crate) struct ServerAggregatorRunner<B, H, S> {
    aggregator: Arc<tokio::sync::Mutex<Aggregator<B, H>>>,
    sleeper: Arc<tokio::sync::Mutex<S>>,
}

/// Server-level group aggregation options.
///
/// Reference: Asynq v0.26.0 `Config.GroupGracePeriod`, `GroupMaxDelay`, and
/// `GroupMaxSize` configure aggregation checks for every server queue:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L215-L238>.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerAggregationConfig {
    group_grace_period: Duration,
    group_max_delay: Duration,
    group_max_size: isize,
}

impl ServerAggregationConfig {
    pub fn new(
        group_grace_period: Duration,
        group_max_delay: Duration,
        group_max_size: isize,
    ) -> Self {
        // Reference: Asynq v0.26.0 `NewServerFromRedisClient` defaults an
        // unset group grace period to one minute and rejects nonzero values
        // below one second.
        // <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L442-L449>.
        let group_grace_period = if group_grace_period.is_zero() {
            DEFAULT_SERVER_GROUP_GRACE_PERIOD
        } else {
            assert!(
                group_grace_period >= Duration::from_secs(1),
                "GroupGracePeriod cannot be less than a second"
            );
            group_grace_period
        };
        Self {
            group_grace_period,
            group_max_delay,
            group_max_size,
        }
    }

    pub fn group_grace_period(&self) -> Duration {
        self.group_grace_period
    }

    pub fn group_max_delay(&self) -> Duration {
        self.group_max_delay
    }

    pub fn group_max_size(&self) -> isize {
        self.group_max_size
    }

    pub fn group_config(
        &self,
        queue: impl Into<String>,
    ) -> Result<AggregationGroupConfig, AggregationError> {
        AggregationGroupConfig::new(
            queue,
            self.group_grace_period,
            self.group_max_delay,
            self.group_max_size,
        )
    }
}

impl<B, H, S> ServerAggregatorRunner<B, H, S> {
    pub fn new(aggregator: Aggregator<B, H>, sleeper: S) -> Self {
        Self {
            aggregator: Arc::new(tokio::sync::Mutex::new(aggregator)),
            sleeper: Arc::new(tokio::sync::Mutex::new(sleeper)),
        }
    }
}

impl<B, H, S> std::fmt::Debug for ServerAggregatorRunner<B, H, S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServerAggregatorRunner")
            .finish_non_exhaustive()
    }
}

impl<B, H, S> AggregationRunner for ServerAggregatorRunner<B, H, S>
where
    B: AggregationBroker + Clone + Send + 'static,
    H: AggregationHandler + Send + 'static,
    S: Sleeper + Send + 'static,
{
    fn run_until_stopped(
        &self,
        shutdown: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<Result<AggregatorRun, ServerError>> {
        let aggregator = Arc::clone(&self.aggregator);
        let sleeper = Arc::clone(&self.sleeper);
        tokio::spawn(async move {
            let mut aggregator = aggregator.lock().await;
            let mut sleeper = sleeper.lock().await;
            aggregator
                .run_until_stopped(&mut *sleeper, shutdown)
                .await
                .map_err(ServerError::from)
        })
    }
}
