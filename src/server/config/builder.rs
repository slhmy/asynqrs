use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;

use super::{Config, ServerProcessingScope};
use crate::server::{
    HealthCheckFunc, HealthCheckHandler, LogLevel, Logger, QueuePriority, SharedGroupAggregator,
};
use crate::{
    ProcessingScope, QueueName, QueueNameError, SharedErrorHandler, SharedIsFailure,
    SharedRetryDelay,
};

/// Error returned when building server processing configuration.
///
/// Reference: Asynq v0.26.0 validates some `Config` combinations during server
/// construction; the Rust builder exposes those failures at build time.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ConfigBuildError {
    #[error("group grace period cannot be less than a second")]
    GroupGracePeriodTooShort,
}

/// Builder for server processing configuration.
///
/// Reference: Asynq v0.26.0 public `Config` stores server runtime knobs while
/// server construction later normalizes zero values:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L96-L255>.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConfigBuilder {
    config: Config,
}

impl ConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.config.concurrency = isize::try_from(concurrency).unwrap_or(isize::MAX);
        self
    }

    pub fn processing_scope<F>(mut self, processing_scope: F) -> Self
    where
        F: Fn() -> ProcessingScope + Send + Sync + 'static,
    {
        self.config.processing_scope = Some(Arc::new(processing_scope));
        self
    }

    pub fn shared_processing_scope(mut self, processing_scope: Arc<ServerProcessingScope>) -> Self {
        self.config.processing_scope = Some(processing_scope);
        self
    }

    pub fn queue<P>(mut self, queue: QueueName, priority: P) -> Self
    where
        P: QueuePriority,
    {
        if let Some(priority) = priority.into_queue_priority() {
            self.config.queues.insert(
                queue.into_string(),
                isize::try_from(priority).unwrap_or(isize::MAX),
            );
        }
        self
    }

    pub fn try_queue<P>(self, queue: impl Into<String>, priority: P) -> Result<Self, QueueNameError>
    where
        P: QueuePriority,
    {
        Ok(self.queue(QueueName::new(queue)?, priority))
    }

    pub fn queues<I, P>(mut self, queues: I) -> Self
    where
        I: IntoIterator<Item = (QueueName, P)>,
        P: QueuePriority,
    {
        for (queue, priority) in queues {
            if let Some(priority) = priority.into_queue_priority() {
                self.config.queues.insert(
                    queue.into_string(),
                    isize::try_from(priority).unwrap_or(isize::MAX),
                );
            }
        }
        self
    }

    pub fn strict_priority(mut self) -> Self {
        self.config.strict_priority = true;
        self
    }

    pub fn weighted_priority(mut self) -> Self {
        self.config.strict_priority = false;
        self
    }

    pub fn task_check_interval(mut self, interval: Duration) -> Self {
        self.config.task_check_interval = interval;
        self
    }

    pub fn retry_delay_func(mut self, retry_delay_func: SharedRetryDelay) -> Self {
        self.config.retry_delay_func = Some(retry_delay_func);
        self
    }

    pub fn is_failure(mut self, is_failure: SharedIsFailure) -> Self {
        self.config.is_failure = Some(is_failure);
        self
    }

    pub fn error_handler(mut self, error_handler: SharedErrorHandler) -> Self {
        self.config.error_handler = Some(error_handler);
        self
    }

    pub fn log_level(mut self, log_level: LogLevel) -> Self {
        self.config.log_level = Some(log_level);
        self
    }

    pub fn logger<L>(self, logger: L) -> Self
    where
        L: Logger + 'static,
    {
        self.shared_logger(Arc::new(logger))
    }

    pub fn shared_logger(mut self, logger: Arc<dyn Logger>) -> Self {
        self.config.logger = Some(logger);
        self
    }

    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.config.shutdown_timeout = timeout;
        self
    }

    pub fn health_check_handler<H>(self, handler: H) -> Self
    where
        H: HealthCheckHandler + 'static,
    {
        self.shared_health_check_handler(Arc::new(handler))
    }

    pub fn shared_health_check_handler(mut self, handler: Arc<dyn HealthCheckHandler>) -> Self {
        self.config.health_check_func = Some(handler);
        self
    }

    pub fn health_check_fn<F>(self, handler: F) -> Self
    where
        F: Fn(Result<(), String>) + Send + Sync + 'static,
    {
        self.health_check_handler(HealthCheckFunc(handler))
    }

    pub fn health_check_interval(mut self, interval: Duration) -> Self {
        self.config.health_check_interval = interval;
        self
    }

    pub fn delayed_task_check_interval(mut self, interval: Duration) -> Self {
        self.config.delayed_task_check_interval = interval;
        self
    }

    pub fn group_grace_period(mut self, period: Duration) -> Self {
        self.config.group_grace_period = period;
        self
    }

    pub fn group_max_delay(mut self, delay: Duration) -> Self {
        self.config.group_max_delay = delay;
        self
    }

    pub fn group_max_size(mut self, size: usize) -> Self {
        self.config.group_max_size = isize::try_from(size).unwrap_or(isize::MAX);
        self
    }

    pub fn group_aggregator(mut self, aggregator: SharedGroupAggregator) -> Self {
        self.config.group_aggregator = Some(aggregator);
        self
    }

    pub fn janitor_interval(mut self, interval: Duration) -> Self {
        self.config.janitor_interval = interval;
        self
    }

    pub fn janitor_batch_size(mut self, size: usize) -> Self {
        self.config.janitor_batch_size = isize::try_from(size).unwrap_or(isize::MAX);
        self
    }

    /// Builds a validated server config.
    pub fn try_build(self) -> Result<Config, ConfigBuildError> {
        if !self.config.group_grace_period.is_zero()
            && self.config.group_grace_period < Duration::from_secs(1)
        {
            return Err(ConfigBuildError::GroupGracePeriodTooShort);
        }
        Ok(self.config)
    }

    pub fn build(self) -> Config {
        self.try_build()
            .expect("server config builder produced invalid configuration")
    }
}

impl From<ConfigBuilder> for Config {
    fn from(builder: ConfigBuilder) -> Self {
        builder.build()
    }
}

impl From<Config> for ConfigBuilder {
    fn from(config: Config) -> Self {
        Self { config }
    }
}
