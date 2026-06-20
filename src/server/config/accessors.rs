use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use super::{Config, ServerProcessingScope};
use crate::server::{HealthCheckHandler, LogLevel, Logger, SharedGroupAggregator};
use crate::{SharedErrorHandler, SharedIsFailure, SharedRetryDelay};

impl Config {
    /// Reference: Asynq v0.26.0 public `Config.Concurrency` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L96-L255>.
    pub fn concurrency(&self) -> isize {
        self.concurrency
    }

    /// Reference: Asynq v0.26.0 public `Config.BaseContext` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L103-L109>.
    pub fn processing_scope(&self) -> Option<&Arc<ServerProcessingScope>> {
        self.processing_scope.as_ref()
    }

    /// Reference: Asynq v0.26.0 public `Config.Queues` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L96-L255>.
    pub fn queues(&self) -> &HashMap<String, isize> {
        &self.queues
    }

    /// Reference: Asynq v0.26.0 public `Config.StrictPriority` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L96-L255>.
    pub fn strict_priority(&self) -> bool {
        self.strict_priority
    }

    /// Reference: Asynq v0.26.0 public `Config.TaskCheckInterval` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L96-L255>.
    pub fn task_check_interval(&self) -> Duration {
        self.task_check_interval
    }

    /// Reference: Asynq v0.26.0 public `Config.RetryDelayFunc` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L119-L123>.
    pub fn retry_delay_func(&self) -> Option<&SharedRetryDelay> {
        self.retry_delay_func.as_ref()
    }

    /// Reference: Asynq v0.26.0 public `Config.IsFailure` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L124-L130>.
    pub fn is_failure(&self) -> Option<&SharedIsFailure> {
        self.is_failure.as_ref()
    }

    /// Reference: Asynq v0.26.0 public `Config.ErrorHandler` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L132-L149>.
    pub fn error_handler(&self) -> Option<&SharedErrorHandler> {
        self.error_handler.as_ref()
    }

    /// Reference: Asynq v0.26.0 public `Config.LogLevel` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L96-L255>.
    pub fn log_level(&self) -> Option<LogLevel> {
        self.log_level
    }

    /// Reference: Asynq v0.26.0 public `Config.Logger` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L180-L184>.
    pub fn logger(&self) -> Option<&Arc<dyn Logger>> {
        self.logger.as_ref()
    }

    /// Reference: Asynq v0.26.0 public `Config.HealthCheckFunc` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L204-L211>.
    pub fn health_check_func(&self) -> Option<&Arc<dyn HealthCheckHandler>> {
        self.health_check_func.as_ref()
    }

    /// Reference: Asynq v0.26.0 public `Config.ShutdownTimeout` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L96-L255>.
    pub fn shutdown_timeout(&self) -> Duration {
        self.shutdown_timeout
    }

    /// Reference: Asynq v0.26.0 public `Config.HealthCheckInterval` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L96-L255>.
    pub fn health_check_interval(&self) -> Duration {
        self.health_check_interval
    }

    /// Reference: Asynq v0.26.0 public `Config.DelayedTaskCheckInterval`
    /// field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L96-L255>.
    pub fn delayed_task_check_interval(&self) -> Duration {
        self.delayed_task_check_interval
    }

    /// Reference: Asynq v0.26.0 public `Config.GroupGracePeriod` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L96-L255>.
    pub fn group_grace_period(&self) -> Duration {
        self.group_grace_period
    }

    /// Reference: Asynq v0.26.0 public `Config.GroupMaxDelay` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L96-L255>.
    pub fn group_max_delay(&self) -> Duration {
        self.group_max_delay
    }

    /// Reference: Asynq v0.26.0 public `Config.GroupMaxSize` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L96-L255>.
    pub fn group_max_size(&self) -> isize {
        self.group_max_size
    }

    /// Reference: Asynq v0.26.0 public `Config.GroupAggregator` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L239-L244>.
    pub fn group_aggregator(&self) -> Option<&SharedGroupAggregator> {
        self.group_aggregator.as_ref()
    }

    /// Reference: Asynq v0.26.0 public `Config.JanitorInterval` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L96-L255>.
    pub fn janitor_interval(&self) -> Duration {
        self.janitor_interval
    }

    /// Reference: Asynq v0.26.0 public `Config.JanitorBatchSize` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L96-L255>.
    pub fn janitor_batch_size(&self) -> isize {
        self.janitor_batch_size
    }
}
