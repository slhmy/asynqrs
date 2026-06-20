use std::sync::Arc;
use std::time::Duration;

use chrono_tz::Tz;

use super::Scheduler;
use crate::{BrokerError, EnqueuePlan, EnqueuePlanError, LogLevel, Logger};

use crate::scheduler::defaults::normalize_scheduler_heartbeat_interval;
use crate::scheduler::entry::SchedulerEntry;
use crate::scheduler::options::SchedulerOpts;

impl<B, C> Scheduler<B, C> {
    pub fn with_shared_connection(mut self) -> Self {
        // Reference: Asynq v0.26.0 `NewSchedulerFromRedisClient` uses a
        // caller-owned Redis client, so scheduler shutdown must not close the
        // underlying connection pool:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L76-L89>.
        self.shared_connection = true;
        self
    }

    pub fn with_tick_interval(mut self, tick_interval: Duration) -> Self {
        self.tick_interval = tick_interval;
        self
    }

    pub fn with_metadata_ttl(mut self, metadata_ttl: Duration) -> Self {
        self.metadata_ttl = metadata_ttl;
        self
    }

    /// Configures the timezone used when evaluating cron scheduler entries.
    ///
    /// Reference: Asynq v0.26.0 `SchedulerOpts.Location` is passed to
    /// robfig/cron via `cron.WithLocation` so cron specs fire in that location:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L100-L103>.
    pub fn with_timezone(mut self, timezone: Tz) -> Self {
        self.timezone = timezone;
        self
    }

    /// Applies supported upstream scheduler construction options.
    ///
    /// Reference: Asynq v0.26.0 `newScheduler` normalizes
    /// `SchedulerOpts.HeartbeatInterval` and defaults `SchedulerOpts.Location`
    /// to UTC:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L80-L103>.
    pub fn with_scheduler_opts(mut self, opts: SchedulerOpts) -> Self {
        self = self.with_heartbeat_interval(opts.heartbeat_interval);
        if let Some(log_level) = opts.log_level {
            self = self.with_log_level(log_level);
        }
        if let Some(logger) = opts.logger {
            self = self.with_logger(logger);
        }
        if let Some(location) = opts.location {
            self = self.with_timezone(location);
        }
        self.pre_enqueue_hook = opts.pre_enqueue_hook;
        self.post_enqueue_hook = opts.post_enqueue_hook;
        self.enqueue_error_hook = opts.enqueue_error_hook;
        self.enqueue_plan_error_hook = opts.enqueue_plan_error_hook;
        self
    }

    /// Configures the scheduler log level.
    ///
    /// Reference: Asynq v0.26.0 `SchedulerOpts.LogLevel` defaults
    /// `LogLevel::Unspecified` to `LogLevel::Info`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L91-L96>.
    pub fn with_log_level(mut self, log_level: LogLevel) -> Self {
        self.log_level = match log_level {
            LogLevel::Unspecified => LogLevel::Info,
            log_level => log_level,
        };
        self
    }

    /// Configures the scheduler logger.
    ///
    /// Reference: Asynq v0.26.0 `SchedulerOpts.Logger` installs the logger
    /// used by scheduler runtime components:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L113-L118>.
    pub fn with_logger(mut self, logger: Arc<dyn Logger>) -> Self {
        self.logger = Some(logger);
        self
    }

    /// Configures the interval between scheduler heartbeat metadata writes.
    ///
    /// Reference: Asynq v0.26.0 `SchedulerOpts.HeartbeatInterval` controls
    /// how often scheduler entries are written to Redis, and heartbeat writes
    /// use twice this interval as the Redis TTL:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L110-L118>.
    pub fn with_heartbeat_interval(mut self, heartbeat_interval: Duration) -> Self {
        self.heartbeat_interval = normalize_scheduler_heartbeat_interval(heartbeat_interval);
        self.metadata_ttl = self.heartbeat_interval.saturating_mul(2);
        self
    }

    /// Configures a callback invoked before a due scheduler entry is enqueued.
    ///
    /// Reference: Asynq v0.26.0 `SchedulerOpts.PreEnqueueFunc`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L146-L153>.
    pub fn with_pre_enqueue_hook<F>(mut self, hook: F) -> Self
    where
        F: Fn(&SchedulerEntry, &EnqueuePlan) + Send + Sync + 'static,
    {
        self.pre_enqueue_hook = Some(Arc::new(hook));
        self
    }

    /// Configures a callback invoked after a scheduler enqueue attempt.
    ///
    /// Reference: Asynq v0.26.0 `SchedulerOpts.PostEnqueueFunc`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L155-L164>.
    pub fn with_post_enqueue_hook<F>(mut self, hook: F) -> Self
    where
        F: Fn(&SchedulerEntry, &EnqueuePlan, Result<(), &BrokerError>) + Send + Sync + 'static,
    {
        self.post_enqueue_hook = Some(Arc::new(hook));
        self
    }

    /// Configures a callback invoked after a scheduled enqueue fails.
    ///
    /// Reference: Asynq v0.26.0 `SchedulerOpts.EnqueueErrorHandler` is called
    /// after `PostEnqueueFunc` when `Client.Enqueue` returns an error:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L126-L136>.
    pub fn with_enqueue_error_hook<F>(mut self, hook: F) -> Self
    where
        F: Fn(&SchedulerEntry, &EnqueuePlan, &BrokerError) + Send + Sync + 'static,
    {
        self.enqueue_error_hook = Some(Arc::new(hook));
        self
    }

    /// Configures a callback invoked when a scheduled enqueue cannot build an
    /// `EnqueuePlan`.
    ///
    /// Reference: Asynq v0.26.0 scheduler jobs return from the current cron
    /// job when `Client.Enqueue` fails, while other due jobs keep running:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L158-L165>.
    pub fn with_enqueue_plan_error_hook<F>(mut self, hook: F) -> Self
    where
        F: Fn(&SchedulerEntry, &EnqueuePlanError) + Send + Sync + 'static,
    {
        self.enqueue_plan_error_hook = Some(Arc::new(hook));
        self
    }
}
