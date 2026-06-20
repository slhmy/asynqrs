//! Redis statistics script result parsing.
//!
//! Reference: Asynq v0.26.0 Inspector statistics operations:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L193-L274>.

use std::time::{Duration, SystemTime};

use crate::admin::{QueueStateSnapshot, QueueStatsSnapshot, QueueThroughputSnapshot};
use crate::broker::redis::keys;
use crate::{AdminError, DailyStats, GroupStats, QueueStats};

use super::time::{signed_duration_nanos, system_time_from_unix_nanoseconds};
use super::value::{i64_to_usize, redis_value_to_i64, redis_value_to_string};

/// Parses Redis script output for current queue statistics.
///
/// Reference: Asynq v0.26.0 `RDB.CurrentStats`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L193-L274>.
pub(in crate::broker::redis::broker) fn parse_queue_stats_values(
    queue: &str,
    now: SystemTime,
    latency_now: SystemTime,
    memory_usage: i64,
    value: redis::Value,
) -> Result<QueueStats, AdminError> {
    let values = match value {
        redis::Value::Array(values) => values,
        value => {
            return Err(AdminError::Other(format!(
                "unexpected current queue stats script result: {value:?}"
            )));
        }
    };
    if values.len() % 2 != 0 {
        return Err(AdminError::Other(format!(
            "current queue stats script returned odd number of values: {}",
            values.len()
        )));
    }

    let mut pending = 0;
    let mut active = 0;
    let mut scheduled = 0;
    let mut retry = 0;
    let mut archived = 0;
    let mut completed = 0;
    let mut aggregating = 0;
    let mut processed = 0;
    let mut failed = 0;
    let mut processed_total = 0;
    let mut failed_total = 0;
    let mut paused = false;
    let mut groups = 0;
    let mut latency = Duration::ZERO;
    let mut latency_nanos = 0_i128;

    for pair in values.chunks_exact(2) {
        let key = redis_value_to_string(&pair[0])?;
        let count = redis_value_to_i64(&pair[1])?;
        match key.as_str() {
            key if key == keys::pending_key(queue) => pending = i64_to_usize(count, key)?,
            key if key == keys::active_key(queue) => active = i64_to_usize(count, key)?,
            key if key == keys::scheduled_key(queue) => {
                scheduled = i64_to_usize(count, key)?;
            }
            key if key == keys::retry_key(queue) => retry = i64_to_usize(count, key)?,
            key if key == keys::archived_key(queue) => {
                archived = i64_to_usize(count, key)?;
            }
            key if key == keys::completed_key(queue) => {
                completed = i64_to_usize(count, key)?;
            }
            key if key == keys::processed_key(queue, now) => {
                processed = i64_to_usize(count, key)?;
            }
            key if key == keys::failed_key(queue, now) => {
                failed = i64_to_usize(count, key)?;
            }
            key if key == keys::processed_total_key(queue) => {
                processed_total = i64_to_usize(count, key)?;
            }
            key if key == keys::failed_total_key(queue) => {
                failed_total = i64_to_usize(count, key)?;
            }
            key if key == keys::paused_key(queue) => paused = count != 0,
            "oldest_pending_since" => {
                if count > 0 {
                    latency_nanos = signed_duration_nanos(
                        latency_now,
                        system_time_from_unix_nanoseconds(count),
                    );
                    if latency_nanos > 0 {
                        latency = Duration::from_nanos(latency_nanos.min(u64::MAX as i128) as u64);
                    }
                }
            }
            "group_size" => groups = i64_to_usize(count, "group_size")?,
            "aggregating_count" => aggregating = i64_to_usize(count, "aggregating_count")?,
            _ => {}
        }
    }

    Ok(QueueStats::with_signed_latency(
        QueueStatsSnapshot {
            queue: queue.to_owned(),
            memory_usage,
            paused,
            states: QueueStateSnapshot {
                groups,
                pending,
                active,
                scheduled,
                retry,
                archived,
                completed,
                aggregating,
            },
            throughput: QueueThroughputSnapshot {
                processed,
                failed,
                processed_total,
                failed_total,
            },
            latency,
            timestamp: now,
        },
        latency_nanos,
    ))
}

pub(in crate::broker::redis::broker) fn current_queue_stats_has_pending_latency(
    value: &redis::Value,
) -> Result<bool, AdminError> {
    let values = match value {
        redis::Value::Array(values) => values,
        value => {
            return Err(AdminError::Other(format!(
                "unexpected current queue stats script result: {value:?}"
            )));
        }
    };
    if values.len() % 2 != 0 {
        return Err(AdminError::Other(format!(
            "current queue stats script returned odd number of values: {}",
            values.len()
        )));
    }

    for pair in values.chunks_exact(2) {
        if redis_value_to_string(&pair[0])? == "oldest_pending_since" {
            return Ok(redis_value_to_i64(&pair[1])? > 0);
        }
    }
    Ok(false)
}

/// Parses Redis script output for aggregation group statistics.
///
/// Reference: Asynq v0.26.0 `Inspector.Groups` and group stat reads:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L62-L76>.
pub(in crate::broker::redis::broker) fn parse_group_stats_values(
    value: redis::Value,
) -> Result<Vec<GroupStats>, AdminError> {
    let values = match value {
        redis::Value::Array(values) => values,
        value => {
            return Err(AdminError::Other(format!(
                "unexpected group stats script result: {value:?}"
            )));
        }
    };
    if values.len() % 2 != 0 {
        return Err(AdminError::Other(format!(
            "group stats script returned odd number of values: {}",
            values.len()
        )));
    }

    let mut stats = Vec::with_capacity(values.len() / 2);
    for pair in values.chunks_exact(2) {
        stats.push(GroupStats::new(
            redis_value_to_string(&pair[0])?,
            i64_to_usize(redis_value_to_i64(&pair[1])?, "group size")?,
        ));
    }
    Ok(stats)
}

/// Parses Redis script output for historical queue statistics.
///
/// Reference: Asynq v0.26.0 `RDB.HistoricalStats`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go>.
pub(in crate::broker::redis::broker) fn parse_historical_queue_stats_values(
    queue: &str,
    days: &[SystemTime],
    value: redis::Value,
) -> Result<Vec<DailyStats>, AdminError> {
    let values = match value {
        redis::Value::Array(values) => values,
        value => {
            return Err(AdminError::Other(format!(
                "unexpected historical queue stats script result: {value:?}"
            )));
        }
    };
    if values.len() != days.len() * 2 {
        return Err(AdminError::Other(format!(
            "historical queue stats script returned {} values for {} days",
            values.len(),
            days.len()
        )));
    }

    let mut stats = Vec::with_capacity(days.len());
    for (index, pair) in values.chunks_exact(2).enumerate() {
        stats.push(DailyStats::new(
            queue.to_owned(),
            i64_to_usize(redis_value_to_i64(&pair[0])?, "historical processed")?,
            i64_to_usize(redis_value_to_i64(&pair[1])?, "historical failed")?,
            days[index],
        ));
    }
    Ok(stats)
}
