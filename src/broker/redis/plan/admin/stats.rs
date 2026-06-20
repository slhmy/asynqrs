//! Redis admin command plans for queue and group statistics.
//!
//! Reference: Asynq v0.26.0 Inspector queue and group stats commands:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go>.

use std::time::{Duration, SystemTime};

use crate::broker::redis::keys;

use super::super::{RedisAdminPlanError, RedisArg, RedisScript, RedisScriptCall};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisCurrentQueueStatsPlan {
    call: RedisScriptCall,
    queue: String,
}

impl RedisCurrentQueueStatsPlan {
    pub fn from_queue(queue: &str, now: SystemTime) -> Result<Self, RedisAdminPlanError> {
        if queue.trim().is_empty() {
            return Err(RedisAdminPlanError::EmptyQueueName);
        }
        Ok(Self {
            call: RedisScriptCall::new(
                RedisScript::CurrentQueueStats,
                vec![
                    keys::pending_key(queue),
                    keys::active_key(queue),
                    keys::scheduled_key(queue),
                    keys::retry_key(queue),
                    keys::archived_key(queue),
                    keys::completed_key(queue),
                    keys::processed_key(queue, now),
                    keys::failed_key(queue, now),
                    keys::processed_total_key(queue),
                    keys::failed_total_key(queue),
                    keys::paused_key(queue),
                    keys::all_groups_key(queue),
                ],
                vec![
                    RedisArg::String(keys::task_key_prefix(queue)),
                    RedisArg::String(keys::group_key_prefix(queue)),
                ],
            ),
            queue: queue.to_owned(),
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }

    pub fn queue(&self) -> &str {
        &self.queue
    }
}

/// Redis command intent for reading queue group statistics.
///
/// Reference: Asynq v0.26.0 `RDB.GroupStats`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisGroupStatsPlan {
    call: RedisScriptCall,
}

impl RedisGroupStatsPlan {
    pub fn from_queue(queue: &str) -> Result<Self, RedisAdminPlanError> {
        // Reference: Asynq v0.26.0 `RDB.GroupStats` derives all-groups and
        // group-prefix keys directly from `qname`:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L578-L598>.
        Ok(Self {
            call: RedisScriptCall::new(
                RedisScript::GroupStats,
                vec![keys::all_groups_key(queue)],
                vec![RedisArg::String(keys::group_key_prefix(queue))],
            ),
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}

/// Redis command intent for reading historical daily queue statistics.
///
/// Reference: Asynq v0.26.0 `RDB.HistoricalStats`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisHistoricalQueueStatsPlan {
    call: RedisScriptCall,
    queue: String,
    days: Vec<SystemTime>,
}

impl RedisHistoricalQueueStatsPlan {
    pub fn from_queue(
        queue: &str,
        now: SystemTime,
        days: usize,
    ) -> Result<Self, RedisAdminPlanError> {
        if queue.trim().is_empty() {
            return Err(RedisAdminPlanError::EmptyQueueName);
        }
        if days == 0 {
            return Err(RedisAdminPlanError::NonPositiveDays);
        }

        let mut keys = Vec::with_capacity(days * 2);
        let mut day_times = Vec::with_capacity(days);
        for offset in 0..days {
            let offset_seconds = offset
                .checked_mul(24 * 60 * 60)
                .ok_or(RedisAdminPlanError::HistoricalDayOverflow)?;
            let day = now
                .checked_sub(Duration::from_secs(offset_seconds as u64))
                .ok_or(RedisAdminPlanError::HistoricalDayOverflow)?;
            keys.push(keys::processed_key(queue, day));
            keys.push(keys::failed_key(queue, day));
            day_times.push(day);
        }

        Ok(Self {
            call: RedisScriptCall::new(RedisScript::HistoricalQueueStats, keys, Vec::new()),
            queue: queue.to_owned(),
            days: day_times,
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }

    pub fn queue(&self) -> &str {
        &self.queue
    }

    pub fn days(&self) -> &[SystemTime] {
        &self.days
    }
}

/// Redis command intent for estimating queue memory usage.
///
/// Reference: Asynq v0.26.0 `RDB.memoryUsage`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisQueueMemoryUsagePlan {
    call: RedisScriptCall,
}

impl RedisQueueMemoryUsagePlan {
    pub fn from_queue(queue: &str) -> Result<Self, RedisAdminPlanError> {
        if queue.trim().is_empty() {
            return Err(RedisAdminPlanError::EmptyQueueName);
        }
        Ok(Self {
            call: RedisScriptCall::new(
                RedisScript::QueueMemoryUsage,
                vec![
                    keys::active_key(queue),
                    keys::pending_key(queue),
                    keys::scheduled_key(queue),
                    keys::retry_key(queue),
                    keys::archived_key(queue),
                    keys::completed_key(queue),
                    keys::all_groups_key(queue),
                ],
                vec![
                    RedisArg::String(keys::task_key_prefix(queue)),
                    RedisArg::I64(20),
                    RedisArg::I64(5),
                    RedisArg::String(keys::group_key_prefix(queue)),
                ],
            ),
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}
