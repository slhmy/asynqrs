use std::time::Duration;

use super::Config;
use crate::server::{
    DEFAULT_JANITOR_BATCH_SIZE, DEFAULT_SERVER_FORWARDER_INTERVAL,
    DEFAULT_SERVER_GROUP_GRACE_PERIOD, DEFAULT_SERVER_HEALTH_CHECK_INTERVAL,
    DEFAULT_SERVER_IDLE_SLEEP, DEFAULT_SERVER_JANITOR_INTERVAL, DEFAULT_SERVER_SHUTDOWN_TIMEOUT,
    LogLevel, QueueSelector, ServerAggregationConfig, ServerError, normalize_interval,
};

impl Config {
    /// Returns the effective worker concurrency after applying Asynq defaults.
    ///
    /// Reference: Asynq v0.26.0 `NewServerFromRedisClient` uses
    /// `runtime.NumCPU()` when `Config.Concurrency` is less than one:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L449-L452>.
    pub(crate) fn effective_concurrency(&self) -> usize {
        if self.concurrency < 1 {
            return std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1);
        }
        usize::try_from(self.concurrency).unwrap_or(usize::MAX)
    }

    /// Returns the effective queue selector after applying Asynq queue rules.
    ///
    /// Reference: Asynq v0.26.0 `NewServerFromRedisClient` ignores invalid
    /// queue names and non-positive priorities, then falls back to the
    /// `default` queue:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L467-L478>.
    pub(crate) fn queue_selector(&self) -> Result<QueueSelector, ServerError> {
        let queues = self
            .queues
            .iter()
            .map(|(name, priority)| (name.clone(), *priority));
        if self.strict_priority {
            QueueSelector::strict_priority(queues)
        } else {
            QueueSelector::weighted_priority(queues)
        }
    }

    pub(crate) fn effective_task_check_interval(&self) -> Duration {
        normalize_interval(self.task_check_interval, DEFAULT_SERVER_IDLE_SLEEP)
    }

    pub(crate) fn effective_log_level(&self) -> LogLevel {
        match self.log_level.unwrap_or(LogLevel::Unspecified) {
            LogLevel::Unspecified => LogLevel::Info,
            log_level => log_level,
        }
    }

    pub(crate) fn effective_shutdown_timeout(&self) -> Duration {
        normalize_interval(self.shutdown_timeout, DEFAULT_SERVER_SHUTDOWN_TIMEOUT)
    }

    pub(crate) fn effective_health_check_interval(&self) -> Duration {
        normalize_interval(
            self.health_check_interval,
            DEFAULT_SERVER_HEALTH_CHECK_INTERVAL,
        )
    }

    pub(crate) fn effective_delayed_task_check_interval(&self) -> Duration {
        normalize_interval(
            self.delayed_task_check_interval,
            DEFAULT_SERVER_FORWARDER_INTERVAL,
        )
    }

    pub(crate) fn effective_group_grace_period(&self) -> Duration {
        if self.group_grace_period.is_zero() {
            DEFAULT_SERVER_GROUP_GRACE_PERIOD
        } else {
            assert!(
                self.group_grace_period >= Duration::from_secs(1),
                "GroupGracePeriod cannot be less than a second"
            );
            self.group_grace_period
        }
    }

    pub(crate) fn effective_group_max_size(&self) -> isize {
        // Reference: Asynq v0.26.0 passes `Config.GroupMaxSize` through as a
        // Go `int` to the aggregator; only the zero value has special meaning
        // inside aggregation checks.
        // <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L583-L590>.
        self.group_max_size
    }

    pub(crate) fn aggregation_config(&self) -> ServerAggregationConfig {
        ServerAggregationConfig::new(
            self.effective_group_grace_period(),
            self.group_max_delay,
            self.effective_group_max_size(),
        )
    }

    pub(crate) fn aggregation_config_if_enabled(&self) -> Option<ServerAggregationConfig> {
        // Reference: Asynq v0.26.0 only wires group aggregation when
        // `Config.GroupAggregator` is non-nil; group timing and size fields
        // alone do not start or configure an aggregator.
        // <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L458-L465>.
        self.group_aggregator
            .as_ref()
            .map(|_| self.aggregation_config())
    }

    pub(crate) fn effective_janitor_interval(&self) -> Duration {
        normalize_interval(self.janitor_interval, DEFAULT_SERVER_JANITOR_INTERVAL)
    }

    pub(crate) fn effective_janitor_batch_size(&self) -> isize {
        // Reference: Asynq v0.26.0 only replaces a zero
        // `Config.JanitorBatchSize` with `DEFAULT_JANITOR_BATCH_SIZE`; nonzero
        // signed values are passed through to the janitor.
        // <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L581-L594>.
        if self.janitor_batch_size == 0 {
            DEFAULT_JANITOR_BATCH_SIZE
        } else {
            self.janitor_batch_size
        }
    }
}
