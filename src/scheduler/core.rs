use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use chrono_tz::Tz;

use super::defaults::{
    DEFAULT_SCHEDULER_HEARTBEAT_INTERVAL, DEFAULT_SCHEDULER_METADATA_TTL,
    DEFAULT_SCHEDULER_TICK_INTERVAL, DEFAULT_SCHEDULER_TIMEZONE,
};
use super::entry::SchedulerEntry;
use super::options::{
    SchedulerEnqueueErrorHook, SchedulerEnqueueHook, SchedulerEnqueuePlanErrorHook,
    SchedulerPostEnqueueHook,
};
use super::spec::generated_scheduler_id;
use super::{SchedulerError, SchedulerState};
use crate::{LogLevel, Logger, SystemClock};

mod accessors;
mod config;
mod lifecycle;

/// Runtime scheduler for periodic task enqueueing.
///
/// Reference: Asynq v0.26.0 `Scheduler` periodically enqueues registered task
/// entries and records enqueue history:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go>.
///
/// Known upstream gap: scheduler lifecycle is implemented around current
/// periodic-task manager needs; remaining edge semantics stay explicit in tests
/// and changelog known gaps.
#[derive(Clone)]
pub struct Scheduler<B, C = SystemClock> {
    pub(in crate::scheduler) broker: B,
    pub(in crate::scheduler) scheduler_id: String,
    pub(crate) entries: Vec<SchedulerEntry>,
    pub(in crate::scheduler) state: SchedulerState,
    pub(in crate::scheduler) clock: C,
    pub(in crate::scheduler) timezone: Tz,
    pub(in crate::scheduler) tick_interval: Duration,
    pub(crate) heartbeat_interval: Duration,
    pub(crate) metadata_ttl: Duration,
    pub(in crate::scheduler) log_level: LogLevel,
    pub(in crate::scheduler) logger: Option<Arc<dyn Logger>>,
    pub(in crate::scheduler) pre_enqueue_hook: Option<Arc<SchedulerEnqueueHook>>,
    pub(in crate::scheduler) post_enqueue_hook: Option<Arc<SchedulerPostEnqueueHook>>,
    pub(in crate::scheduler) enqueue_error_hook: Option<Arc<SchedulerEnqueueErrorHook>>,
    pub(in crate::scheduler) enqueue_plan_error_hook: Option<Arc<SchedulerEnqueuePlanErrorHook>>,
    pub(in crate::scheduler) shared_connection: bool,
}

impl<B, C> Scheduler<B, C> {
    /// Creates a scheduler with an upstream-style generated scheduler id and
    /// an injected clock.
    ///
    /// Reference: Asynq v0.26.0 `generateSchedulerID` formats scheduler ids as
    /// `hostname:pid:uuid`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L105-L111>.
    pub fn with_generated_id_and_clock(broker: B, clock: C) -> Result<Self, SchedulerError> {
        Self::with_clock(generated_scheduler_id(), broker, clock)
    }

    pub fn with_clock(
        scheduler_id: impl Into<String>,
        broker: B,
        clock: C,
    ) -> Result<Self, SchedulerError> {
        let scheduler_id = scheduler_id.into();
        if scheduler_id.trim().is_empty() {
            return Err(SchedulerError::EmptySchedulerId);
        }
        Ok(Self {
            broker,
            scheduler_id,
            entries: Vec::new(),
            state: SchedulerState::New,
            clock,
            timezone: DEFAULT_SCHEDULER_TIMEZONE,
            tick_interval: DEFAULT_SCHEDULER_TICK_INTERVAL,
            heartbeat_interval: DEFAULT_SCHEDULER_HEARTBEAT_INTERVAL,
            metadata_ttl: DEFAULT_SCHEDULER_METADATA_TTL,
            log_level: LogLevel::Info,
            logger: None,
            pre_enqueue_hook: None,
            post_enqueue_hook: None,
            enqueue_error_hook: None,
            enqueue_plan_error_hook: None,
            shared_connection: false,
        })
    }
}

impl<B, C> fmt::Debug for Scheduler<B, C>
where
    B: fmt::Debug,
    C: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scheduler")
            .field("broker", &self.broker)
            .field("scheduler_id", &self.scheduler_id)
            .field("entries", &self.entries)
            .field("clock", &self.clock)
            .field("timezone", &self.timezone)
            .field("tick_interval", &self.tick_interval)
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field("metadata_ttl", &self.metadata_ttl)
            .field("log_level", &self.log_level)
            .field("has_logger", &self.logger.is_some())
            .field("has_pre_enqueue_hook", &self.pre_enqueue_hook.is_some())
            .field("has_post_enqueue_hook", &self.post_enqueue_hook.is_some())
            .field("has_enqueue_error_hook", &self.enqueue_error_hook.is_some())
            .field(
                "has_enqueue_plan_error_hook",
                &self.enqueue_plan_error_hook.is_some(),
            )
            .field("shared_connection", &self.shared_connection)
            .finish()
    }
}
