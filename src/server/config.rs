use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use super::{HealthCheckHandler, LogLevel, Logger, SharedGroupAggregator};
use crate::{ProcessingScope, SharedErrorHandler, SharedIsFailure, SharedRetryDelay};

mod accessors;
mod builder;
mod effective;

pub use builder::{ConfigBuildError, ConfigBuilder};

/// Callback used to build the outer processing scope for handler invocations.
///
/// Reference: Asynq v0.26.0 public `Config.BaseContext` field:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L103-L109>.
pub type ServerProcessingScope = dyn Fn() -> ProcessingScope + Send + Sync;

/// Server processing configuration.
///
/// Reference: Asynq v0.26.0 public `Config` fields:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L96-L255>.
#[derive(Clone)]
pub struct Config {
    pub(crate) concurrency: isize,
    pub(crate) processing_scope: Option<Arc<ServerProcessingScope>>,
    pub(crate) queues: HashMap<String, isize>,
    pub(crate) strict_priority: bool,
    pub(crate) task_check_interval: Duration,
    pub(crate) retry_delay_func: Option<SharedRetryDelay>,
    pub(crate) is_failure: Option<SharedIsFailure>,
    pub(crate) error_handler: Option<SharedErrorHandler>,
    pub(crate) log_level: Option<LogLevel>,
    pub(crate) logger: Option<Arc<dyn Logger>>,
    pub(crate) shutdown_timeout: Duration,
    pub(crate) health_check_func: Option<Arc<dyn HealthCheckHandler>>,
    pub(crate) health_check_interval: Duration,
    pub(crate) delayed_task_check_interval: Duration,
    pub(crate) group_grace_period: Duration,
    pub(crate) group_max_delay: Duration,
    pub(crate) group_max_size: isize,
    pub(crate) group_aggregator: Option<SharedGroupAggregator>,
    pub(crate) janitor_interval: Duration,
    pub(crate) janitor_batch_size: isize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            concurrency: 0,
            processing_scope: None,
            queues: HashMap::new(),
            strict_priority: false,
            task_check_interval: Duration::ZERO,
            retry_delay_func: None,
            is_failure: None,
            error_handler: None,
            log_level: None,
            logger: None,
            shutdown_timeout: Duration::ZERO,
            health_check_func: None,
            health_check_interval: Duration::ZERO,
            delayed_task_check_interval: Duration::ZERO,
            group_grace_period: Duration::ZERO,
            group_max_delay: Duration::ZERO,
            group_max_size: 0,
            group_aggregator: None,
            janitor_interval: Duration::ZERO,
            janitor_batch_size: 0,
        }
    }
}

impl Config {
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::new()
    }
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("concurrency", &self.concurrency)
            .field("has_processing_scope", &self.processing_scope.is_some())
            .field("queues", &self.queues)
            .field("strict_priority", &self.strict_priority)
            .field("task_check_interval", &self.task_check_interval)
            .field("has_retry_delay_func", &self.retry_delay_func.is_some())
            .field("has_is_failure", &self.is_failure.is_some())
            .field("has_error_handler", &self.error_handler.is_some())
            .field("log_level", &self.log_level)
            .field("has_logger", &self.logger.is_some())
            .field("shutdown_timeout", &self.shutdown_timeout)
            .field("has_health_check_func", &self.health_check_func.is_some())
            .field("health_check_interval", &self.health_check_interval)
            .field(
                "delayed_task_check_interval",
                &self.delayed_task_check_interval,
            )
            .field("group_grace_period", &self.group_grace_period)
            .field("group_max_delay", &self.group_max_delay)
            .field("group_max_size", &self.group_max_size)
            .field("has_group_aggregator", &self.group_aggregator.is_some())
            .field("janitor_interval", &self.janitor_interval)
            .field("janitor_batch_size", &self.janitor_batch_size)
            .finish()
    }
}

impl PartialEq for Config {
    fn eq(&self, other: &Self) -> bool {
        self.concurrency == other.concurrency
            && option_arc_ptr_eq(&self.processing_scope, &other.processing_scope)
            && self.queues == other.queues
            && self.strict_priority == other.strict_priority
            && self.task_check_interval == other.task_check_interval
            && option_shared_retry_delay_ptr_eq(&self.retry_delay_func, &other.retry_delay_func)
            && option_shared_is_failure_ptr_eq(&self.is_failure, &other.is_failure)
            && option_shared_error_handler_ptr_eq(&self.error_handler, &other.error_handler)
            && self.log_level == other.log_level
            && option_arc_ptr_eq(&self.logger, &other.logger)
            && self.shutdown_timeout == other.shutdown_timeout
            && option_arc_ptr_eq(&self.health_check_func, &other.health_check_func)
            && self.health_check_interval == other.health_check_interval
            && self.delayed_task_check_interval == other.delayed_task_check_interval
            && self.group_grace_period == other.group_grace_period
            && self.group_max_delay == other.group_max_delay
            && self.group_max_size == other.group_max_size
            && option_arc_ptr_eq(&self.group_aggregator, &other.group_aggregator)
            && self.janitor_interval == other.janitor_interval
            && self.janitor_batch_size == other.janitor_batch_size
    }
}

impl Eq for Config {}

fn option_arc_ptr_eq<T: ?Sized>(left: &Option<Arc<T>>, right: &Option<Arc<T>>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => Arc::ptr_eq(left, right),
        (None, None) => true,
        _ => false,
    }
}

fn option_shared_retry_delay_ptr_eq(
    left: &Option<SharedRetryDelay>,
    right: &Option<SharedRetryDelay>,
) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => Arc::ptr_eq(&left.0, &right.0),
        (None, None) => true,
        _ => false,
    }
}

fn option_shared_is_failure_ptr_eq(
    left: &Option<SharedIsFailure>,
    right: &Option<SharedIsFailure>,
) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => Arc::ptr_eq(&left.0, &right.0),
        (None, None) => true,
        _ => false,
    }
}

fn option_shared_error_handler_ptr_eq(
    left: &Option<SharedErrorHandler>,
    right: &Option<SharedErrorHandler>,
) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => Arc::ptr_eq(&left.0, &right.0),
        (None, None) => true,
        _ => false,
    }
}
