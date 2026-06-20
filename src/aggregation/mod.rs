use crate::SystemClock;
use crate::server::{LogLevel, Logger};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

mod broker;
mod error;
mod handler;
mod models;
mod runtime;

pub(crate) use broker::AggregationBroker;
pub use error::AggregationError;
pub use handler::{
    AggregationHandler, GroupAggregator, GroupAggregatorFunc, GroupAggregatorHandler,
};
pub(crate) use models::AggregatorRun;
pub use models::{AggregationGroup, AggregationGroupConfig, AggregationSet};

/// Runtime shell for flushing aggregation groups.
///
/// Reference: Asynq v0.26.0 `aggregator` checks group readiness, reads
/// aggregation sets, invokes an aggregate handler, deletes processed sets, and
/// reclaims stale sets:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/aggregator.go>.
///
/// Aggregation checks may run concurrently, but calls into the user aggregation
/// handler are serialized through one async mutex. This preserves a single
/// mutable handler instance while allowing Redis discovery/check work to overlap.
///
#[derive(Clone)]
pub(crate) struct Aggregator<B, H, C = SystemClock> {
    pub(super) broker: B,
    pub(super) handler: Arc<Mutex<H>>,
    pub(super) groups: Vec<AggregationGroup>,
    pub(super) auto_groups: Vec<AggregationGroupConfig>,
    pub(super) clock: C,
    pub(super) tick_interval: Duration,
    pub(super) log_level: LogLevel,
    pub(super) logger: Option<Arc<dyn Logger>>,
}

impl<B, H, C> std::fmt::Debug for Aggregator<B, H, C>
where
    B: std::fmt::Debug,
    H: std::fmt::Debug,
    C: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Aggregator")
            .field("broker", &self.broker)
            .field("handler", &self.handler)
            .field("groups", &self.groups)
            .field("auto_groups", &self.auto_groups)
            .field("clock", &self.clock)
            .field("tick_interval", &self.tick_interval)
            .field("log_level", &self.log_level)
            .field("has_logger", &self.logger.is_some())
            .finish()
    }
}

impl<B, H, C> Aggregator<B, H, C> {
    pub fn with_clock(broker: B, handler: H, clock: C) -> Self {
        Self {
            broker,
            handler: Arc::new(Mutex::new(handler)),
            groups: Vec::new(),
            auto_groups: Vec::new(),
            clock,
            tick_interval: DEFAULT_AGGREGATOR_TICK_INTERVAL,
            log_level: LogLevel::Info,
            logger: None,
        }
    }

    pub(crate) fn with_optional_logger(mut self, logger: Option<Arc<dyn Logger>>) -> Self {
        self.logger = logger;
        self
    }

    /// Configures the aggregation logger severity threshold.
    ///
    /// Reference: Asynq v0.26.0 applies `Config.LogLevel` to the server logger
    /// before constructing the aggregator:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L477-L492>.
    pub fn with_log_level(mut self, log_level: LogLevel) -> Self {
        self.log_level = match log_level {
            LogLevel::Unspecified => LogLevel::Info,
            log_level => log_level,
        };
        self
    }

    pub(crate) fn add_auto_group_config(&mut self, config: AggregationGroupConfig) {
        self.tick_interval = self.tick_interval.min(config.grace_period);
        self.auto_groups.push(config);
    }
}

impl<B, H> Aggregator<B, H, SystemClock> {
    pub fn new(broker: B, handler: H) -> Self {
        Self::with_clock(broker, handler, SystemClock)
    }
}

/// Reference: Asynq v0.26.0 `DEFAULT_AGGREGATOR_TICK_INTERVAL`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/aggregator.go#L46-L49>.
pub(crate) const DEFAULT_AGGREGATOR_TICK_INTERVAL: Duration = Duration::from_secs(7);
/// Maximum number of aggregation checks in flight concurrently.
///
/// Reference: Asynq v0.26.0 `MAX_CONCURRENT_AGGREGATION_CHECKS`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/aggregator.go#L43-L44>.
pub(crate) const MAX_CONCURRENT_AGGREGATION_CHECKS: usize = 3;
#[cfg(test)]
mod tests;
