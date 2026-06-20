use std::time::SystemTime;

use super::super::{RedisAdminPlanError, RedisArg, RedisScript, RedisScriptCall};
use super::unix_seconds_admin;
use crate::broker::redis::keys;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisListQueuesPlan {
    key: String,
}

/// Redis command intent for pausing a queue.
///
/// Reference: Asynq v0.26.0 `RDB.Pause`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisPauseQueuePlan {
    paused_key: String,
    paused_at: i64,
}

/// Redis command intent for unpausing a queue.
///
/// Reference: Asynq v0.26.0 `RDB.Unpause`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisUnpauseQueuePlan {
    paused_key: String,
}

/// Redis command intent for removing a queue.
///
/// Reference: Asynq v0.26.0 `RDB.RemoveQueue` Lua scripts remove pending,
/// active, scheduled, retry, archived, and lease keys; queue-set removal is a
/// separate `SREM` after script success.
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1775-L1852>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisDeleteQueuePlan {
    call: RedisScriptCall,
    queue: String,
}

/// Redis command intent for deleting one non-active task.
impl RedisListQueuesPlan {
    pub fn new() -> Self {
        Self {
            key: keys::ALL_QUEUES.to_owned(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }
}

impl Default for RedisListQueuesPlan {
    fn default() -> Self {
        Self::new()
    }
}

impl RedisPauseQueuePlan {
    pub fn from_queue(queue: &str, now: SystemTime) -> Result<Self, RedisAdminPlanError> {
        if queue.trim().is_empty() {
            return Err(RedisAdminPlanError::EmptyQueueName);
        }
        Ok(Self {
            paused_key: keys::paused_key(queue),
            paused_at: unix_seconds_admin(now, "queue pause time")?,
        })
    }

    pub fn paused_key(&self) -> &str {
        &self.paused_key
    }

    pub fn paused_at(&self) -> i64 {
        self.paused_at
    }
}

impl RedisUnpauseQueuePlan {
    pub fn from_queue(queue: &str) -> Result<Self, RedisAdminPlanError> {
        if queue.trim().is_empty() {
            return Err(RedisAdminPlanError::EmptyQueueName);
        }
        Ok(Self {
            paused_key: keys::paused_key(queue),
        })
    }

    pub fn paused_key(&self) -> &str {
        &self.paused_key
    }
}

impl RedisDeleteQueuePlan {
    pub fn from_queue(queue: &str) -> Result<Self, RedisAdminPlanError> {
        // Reference: Asynq v0.26.0 `RDB.RemoveQueue` checks membership in
        // `asynq:queues`, then derives queue-local keys directly from `qname`:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1886-L1931>.
        Ok(Self {
            call: RedisScriptCall::new(
                RedisScript::DeleteQueue,
                vec![
                    keys::pending_key(queue),
                    keys::active_key(queue),
                    keys::scheduled_key(queue),
                    keys::retry_key(queue),
                    keys::archived_key(queue),
                    keys::lease_key(queue),
                ],
                vec![RedisArg::String(keys::task_key_prefix(queue))],
            ),
            queue: queue.to_owned(),
        })
    }

    pub fn from_queue_and_force(queue: &str, force: bool) -> Result<Self, RedisAdminPlanError> {
        if !force {
            return Self::from_queue(queue);
        }
        // Reference: Asynq v0.26.0 `RDB.RemoveQueue` uses the same key
        // derivation for forced removal, with a different Lua script:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1886-L1931>.
        Ok(Self {
            call: RedisScriptCall::new(
                RedisScript::DeleteQueueForce,
                vec![
                    keys::pending_key(queue),
                    keys::active_key(queue),
                    keys::scheduled_key(queue),
                    keys::retry_key(queue),
                    keys::archived_key(queue),
                    keys::lease_key(queue),
                ],
                vec![RedisArg::String(keys::task_key_prefix(queue))],
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

    pub fn queue_set_key(&self) -> &'static str {
        keys::ALL_QUEUES
    }
}
