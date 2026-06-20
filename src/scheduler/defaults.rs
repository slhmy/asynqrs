//! Scheduler defaults and heartbeat interval helpers.
//!
//! Reference: Asynq v0.26.0 scheduler defaults heartbeat timing and writes
//! heartbeat metadata with a TTL derived from that interval:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L52-L103>
//! and <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L337-L339>.

use std::time::Duration;

use chrono_tz::Tz;

pub const DEFAULT_SCHEDULER_TICK_INTERVAL: Duration = Duration::from_secs(1);
/// Default interval between scheduler heartbeat writes.
///
/// Reference: Asynq v0.26.0 `DEFAULT_SCHEDULER_HEARTBEAT_INTERVAL`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L52-L52>.
pub const DEFAULT_SCHEDULER_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
/// Default TTL for scheduler heartbeat metadata.
///
/// Reference: Asynq v0.26.0 scheduler heartbeat writes entries with
/// `heartbeatInterval*2` TTL:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L337-L339>.
pub const DEFAULT_SCHEDULER_METADATA_TTL: Duration =
    DEFAULT_SCHEDULER_HEARTBEAT_INTERVAL.saturating_mul(2);
/// Default timezone used when evaluating scheduler cron specs.
///
/// Reference: Asynq v0.26.0 `NewScheduler` defaults `SchedulerOpts.Location`
/// to UTC when the caller does not provide one:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L100-L103>.
pub const DEFAULT_SCHEDULER_TIMEZONE: Tz = chrono_tz::UTC;

pub(super) fn scheduler_heartbeat(interval: Duration) -> tokio::time::Interval {
    let mut heartbeat = tokio::time::interval(normalize_scheduler_heartbeat_interval(interval));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat
}

pub(super) fn normalize_scheduler_heartbeat_interval(interval: Duration) -> Duration {
    if interval.is_zero() {
        DEFAULT_SCHEDULER_HEARTBEAT_INTERVAL
    } else {
        interval
    }
}
