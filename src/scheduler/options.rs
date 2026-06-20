//! Scheduler construction options and hook types.
//!
//! Reference: Asynq v0.26.0 public `SchedulerOpts` fields:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L113-L153>.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use chrono_tz::Tz;

use crate::{BrokerError, EnqueuePlan, EnqueuePlanError, LogLevel, Logger};

use super::SchedulerEntry;

pub type SchedulerEnqueueHook = dyn Fn(&SchedulerEntry, &EnqueuePlan) + Send + Sync;
pub type SchedulerPostEnqueueHook =
    dyn Fn(&SchedulerEntry, &EnqueuePlan, Result<(), &BrokerError>) + Send + Sync;
pub type SchedulerEnqueueErrorHook =
    dyn Fn(&SchedulerEntry, &EnqueuePlan, &BrokerError) + Send + Sync;
pub type SchedulerEnqueuePlanErrorHook = dyn Fn(&SchedulerEntry, &EnqueuePlanError) + Send + Sync;

/// Scheduler construction options.
///
/// Reference: Asynq v0.26.0 public `SchedulerOpts` fields:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L113-L153>.
#[derive(Clone)]
pub struct SchedulerOpts {
    pub heartbeat_interval: Duration,
    pub log_level: Option<LogLevel>,
    pub logger: Option<Arc<dyn Logger>>,
    pub location: Option<Tz>,
    pub pre_enqueue_hook: Option<Arc<SchedulerEnqueueHook>>,
    pub post_enqueue_hook: Option<Arc<SchedulerPostEnqueueHook>>,
    pub enqueue_error_hook: Option<Arc<SchedulerEnqueueErrorHook>>,
    /// Rust-specific hook for validation failures that happen before an
    /// `EnqueuePlan` exists.
    ///
    /// Reference: Asynq v0.26.0 scheduler jobs call `Client.Enqueue` and
    /// return from the current cron job when enqueue validation fails:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L158-L165>.
    pub enqueue_plan_error_hook: Option<Arc<SchedulerEnqueuePlanErrorHook>>,
}

impl Default for SchedulerOpts {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::ZERO,
            log_level: None,
            logger: None,
            location: None,
            pre_enqueue_hook: None,
            post_enqueue_hook: None,
            enqueue_error_hook: None,
            enqueue_plan_error_hook: None,
        }
    }
}

impl SchedulerOpts {
    /// Reference: Asynq v0.26.0 public `SchedulerOpts.HeartbeatInterval`
    /// field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L113-L153>.
    pub fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }

    /// Reference: Asynq v0.26.0 public `SchedulerOpts.LogLevel` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L113-L153>.
    pub fn log_level(&self) -> Option<LogLevel> {
        self.log_level
    }

    /// Reference: Asynq v0.26.0 public `SchedulerOpts.Logger` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L113-L153>.
    pub fn logger(&self) -> Option<&Arc<dyn Logger>> {
        self.logger.as_ref()
    }

    /// Reference: Asynq v0.26.0 public `SchedulerOpts.Location` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L113-L153>.
    pub fn location(&self) -> Option<Tz> {
        self.location
    }

    /// Rust-specific accessor for enqueue validation failures that occur
    /// before an `EnqueuePlan` exists.
    ///
    /// Reference: Asynq v0.26.0 scheduler jobs return after `Client.Enqueue`
    /// errors while the cron scheduler continues other due jobs:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L158-L165>.
    pub fn enqueue_plan_error_hook(&self) -> Option<&Arc<SchedulerEnqueuePlanErrorHook>> {
        self.enqueue_plan_error_hook.as_ref()
    }
}

impl fmt::Debug for SchedulerOpts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SchedulerOpts")
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field("log_level", &self.log_level)
            .field("has_logger", &self.logger.is_some())
            .field("location", &self.location)
            .field("has_pre_enqueue_hook", &self.pre_enqueue_hook.is_some())
            .field("has_post_enqueue_hook", &self.post_enqueue_hook.is_some())
            .field("has_enqueue_error_hook", &self.enqueue_error_hook.is_some())
            .field(
                "has_enqueue_plan_error_hook",
                &self.enqueue_plan_error_hook.is_some(),
            )
            .finish()
    }
}
