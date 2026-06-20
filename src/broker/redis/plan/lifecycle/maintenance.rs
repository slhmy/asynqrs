use std::time::{SystemTime, UNIX_EPOCH};

use crate::broker::redis::keys;

use super::super::{
    RedisArg, RedisDeleteExpiredCompletedTasksPlanError, RedisForwardPlanError, RedisScript,
    RedisScriptCall, duration_nanoseconds, unix_seconds_floor,
};

/// Redis command intent for moving due scheduled/retry tasks to processable
/// queues.
///
/// Reference: Asynq v0.26.0 `RDB.ForwardIfReady` and `forwardCmd`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L861-L900>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisForwardPlan {
    call: RedisScriptCall,
}

/// Redis command intent for deleting expired completed tasks.
///
/// Reference: Asynq v0.26.0 `RDB.DeleteExpiredCompletedTasks`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisDeleteExpiredCompletedTasksPlan {
    call: RedisScriptCall,
}

impl RedisForwardPlan {
    pub fn from_scheduled_queue(
        queue: &str,
        now: SystemTime,
    ) -> Result<Self, RedisForwardPlanError> {
        forward_plan(queue, keys::scheduled_key(queue), now)
    }

    pub fn from_retry_queue(queue: &str, now: SystemTime) -> Result<Self, RedisForwardPlanError> {
        forward_plan(queue, keys::retry_key(queue), now)
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}

impl RedisDeleteExpiredCompletedTasksPlan {
    pub fn from_queue(
        queue: &str,
        now: SystemTime,
        batch_size: isize,
    ) -> Result<Self, RedisDeleteExpiredCompletedTasksPlanError> {
        // Reference: Asynq v0.26.0 `RDB.deleteExpiredCompletedTasks` derives
        // cleanup keys directly from `qname` and passes `batchSize` directly
        // to the Lua `LIMIT` count, including signed nonzero values:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1264-L1310>.
        Ok(Self {
            call: RedisScriptCall::new(
                RedisScript::DeleteExpiredCompletedTasks,
                vec![keys::completed_key(queue)],
                vec![
                    RedisArg::I64(unix_seconds_cleanup(now, "completed cleanup scan")?),
                    RedisArg::String(keys::task_key_prefix(queue)),
                    RedisArg::I64(batch_size.try_into().map_err(|_| {
                        RedisDeleteExpiredCompletedTasksPlanError::TimeOverflow(
                            "cleanup batch size",
                        )
                    })?),
                ],
            ),
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}

fn forward_plan(
    queue: &str,
    source_key: String,
    now: SystemTime,
) -> Result<RedisForwardPlan, RedisForwardPlanError> {
    // Reference: Asynq v0.26.0 `forwardAll` derives scheduled/retry,
    // pending, task-prefix, and group-prefix keys directly from `qname`:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1003-L1019>.
    Ok(RedisForwardPlan {
        call: RedisScriptCall::new(
            RedisScript::Forward,
            vec![source_key, keys::pending_key(queue)],
            vec![
                RedisArg::I64(unix_seconds_forward(now, "forward time")?),
                RedisArg::String(keys::task_key_prefix(queue)),
                RedisArg::I64(unix_nanoseconds_forward(now)?),
                RedisArg::String(keys::group_key_prefix(queue)),
            ],
        ),
    })
}

fn unix_seconds_forward(
    time: SystemTime,
    context: &'static str,
) -> Result<i64, RedisForwardPlanError> {
    let seconds = unix_seconds_floor(time);
    seconds
        .try_into()
        .map_err(|_| RedisForwardPlanError::TimeOverflow(context))
}

fn unix_nanoseconds_forward(time: SystemTime) -> Result<i64, RedisForwardPlanError> {
    let nanos = match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration_nanoseconds(duration),
        Err(error) => -duration_nanoseconds(error.duration()),
    };
    nanos
        .try_into()
        .map_err(|_| RedisForwardPlanError::TimeOverflow("unix nanoseconds"))
}

fn unix_seconds_cleanup(
    time: SystemTime,
    context: &'static str,
) -> Result<i64, RedisDeleteExpiredCompletedTasksPlanError> {
    let seconds = unix_seconds_floor(time);
    seconds
        .try_into()
        .map_err(|_| RedisDeleteExpiredCompletedTasksPlanError::TimeOverflow(context))
}
