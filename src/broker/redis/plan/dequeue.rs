//! Redis dequeue command plans.
//!
//! Reference: Asynq v0.26.0 `RDB.Dequeue` and `dequeueCmd`:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L240-L274>.

use std::time::SystemTime;

use crate::broker::redis::keys;

use super::{
    DEFAULT_LEASE_DURATION, RedisArg, RedisDequeueCall, RedisDequeuePlanError, unix_seconds_floor,
};

/// Redis command intent for dequeuing the next pending task.
///
/// Reference: Asynq v0.26.0 `RDB.Dequeue` scans queues and runs `dequeueCmd`
/// to move a task from pending to active with a lease:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L243-L274>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisDequeuePlan {
    queue_calls: Vec<RedisDequeueCall>,
    lease_expires_at: SystemTime,
}

impl RedisDequeuePlan {
    pub fn from_queues(queues: &[String], now: SystemTime) -> Result<Self, RedisDequeuePlanError> {
        let lease_expires_at =
            now.checked_add(DEFAULT_LEASE_DURATION)
                .ok_or(RedisDequeuePlanError::TimeOverflow(
                    "dequeue lease expiration",
                ))?;
        let lease_expires_at_seconds = unix_seconds_checked(lease_expires_at, "dequeue lease")?;

        let mut queue_calls = Vec::with_capacity(queues.len());
        for queue in queues {
            // Reference: Asynq v0.26.0 `RDB.Dequeue` derives each queue's
            // pending, paused, active, lease, and task-prefix keys directly
            // from `qname`:
            // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L240-L274>.
            queue_calls.push(RedisDequeueCall {
                queue: queue.clone(),
                keys: vec![
                    keys::pending_key(queue),
                    keys::paused_key(queue),
                    keys::active_key(queue),
                    keys::lease_key(queue),
                ],
                args: vec![
                    RedisArg::I64(lease_expires_at_seconds),
                    RedisArg::String(keys::task_key_prefix(queue)),
                ],
            });
        }

        Ok(Self {
            queue_calls,
            lease_expires_at,
        })
    }

    pub fn queue_calls(&self) -> &[RedisDequeueCall] {
        &self.queue_calls
    }

    pub fn lease_expires_at(&self) -> SystemTime {
        self.lease_expires_at
    }
}

fn unix_seconds_checked(
    time: SystemTime,
    context: &'static str,
) -> Result<i64, RedisDequeuePlanError> {
    let seconds = unix_seconds_floor(time);
    seconds
        .try_into()
        .map_err(|_| RedisDequeuePlanError::TimeOverflow(context))
}
