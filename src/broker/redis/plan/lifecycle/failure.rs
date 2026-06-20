//! Redis lifecycle plans for failed active tasks.
//!
//! Reference: Asynq v0.26.0 failed-task lifecycle commands:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L380-L477>.

use std::time::SystemTime;

use crate::broker::redis::keys;
use crate::task::TaskMessage;

use super::super::{
    ARCHIVED_EXPIRATION, MAX_ARCHIVE_SIZE, RedisArchivePlanError, RedisArg, RedisRetryPlanError,
    RedisScript, RedisScriptCall, STATS_TTL, encoded_msg_arg, task_key, unix_seconds_floor,
};

/// Redis command intent for archiving a failed active task.
///
/// Reference: Asynq v0.26.0 archive-related task lifecycle handling in
/// `internal/rdb`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisArchivePlan {
    call: RedisScriptCall,
}

impl RedisArchivePlan {
    /// Reference: Asynq v0.26.0 `RDB.Archive` archives failed active tasks
    /// using the RDB clock and without accepting an archive time or
    /// `isFailure` option:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.
    pub fn from_message(
        message: &TaskMessage,
        now: SystemTime,
        error_message: &str,
    ) -> Result<Self, RedisArchivePlanError> {
        // Reference: Asynq v0.26.0 `RDB.Archive` derives keys and script
        // arguments directly from `msg.Queue` and `msg.ID`:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L904-L940>.
        Ok(Self {
            call: archive_call(message, now, error_message)?,
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}

/// Redis command intent for retrying a failed active task.
///
/// Reference: Asynq v0.26.0 `RDB.Retry`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L380-L418>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisRetryPlan {
    call: RedisScriptCall,
}

impl RedisRetryPlan {
    pub fn from_message(
        message: &TaskMessage,
        now: SystemTime,
        retry_at: SystemTime,
        error_message: &str,
        is_failure: bool,
    ) -> Result<Self, RedisRetryPlanError> {
        // Reference: Asynq v0.26.0 `RDB.Retry` derives keys and script
        // arguments directly from `msg.Queue` and `msg.ID`:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L801-L840>.
        Ok(Self {
            call: retry_call(message, now, retry_at, error_message, is_failure)?,
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}

fn archive_call(
    msg: &TaskMessage,
    now: SystemTime,
    error_message: &str,
) -> Result<RedisScriptCall, RedisArchivePlanError> {
    let mut archived_message = msg.clone();
    archived_message.error_msg = error_message.to_owned();
    archived_message.last_failed_at = unix_seconds_archive(now, "last failed time")?;
    let cutoff = now
        .checked_sub(ARCHIVED_EXPIRATION)
        .ok_or(RedisArchivePlanError::TimeOverflow("archive cutoff"))?;

    Ok(RedisScriptCall::new(
        RedisScript::Archive,
        vec![
            task_key(msg),
            keys::active_key(&msg.queue),
            keys::lease_key(&msg.queue),
            keys::archived_key(&msg.queue),
            keys::processed_key(&msg.queue, now),
            keys::failed_key(&msg.queue, now),
            keys::processed_total_key(&msg.queue),
            keys::failed_total_key(&msg.queue),
            keys::task_key_prefix(&msg.queue),
        ],
        vec![
            RedisArg::String(msg.id.clone()),
            encoded_msg_arg(&archived_message),
            RedisArg::I64(unix_seconds_archive(now, "archive time")?),
            RedisArg::I64(unix_seconds_archive(cutoff, "archive cutoff")?),
            RedisArg::I64(MAX_ARCHIVE_SIZE as i64),
            RedisArg::I64(archive_stats_expire_at(now)?),
            RedisArg::I64(i64::MAX),
        ],
    ))
}

fn retry_call(
    msg: &TaskMessage,
    now: SystemTime,
    retry_at: SystemTime,
    error_message: &str,
    is_failure: bool,
) -> Result<RedisScriptCall, RedisRetryPlanError> {
    let mut retry_message = msg.clone();
    if is_failure {
        // Reference: Asynq v0.26.0 `RDB.Retry` increments the Go `int`
        // `TaskMessage.Retried` value before protobuf encoding writes it with
        // a direct `int32(...)` conversion:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L801-L814>.
        retry_message.retried = retry_message.retried.wrapping_add(1);
    }
    retry_message.error_msg = error_message.to_owned();
    retry_message.last_failed_at = unix_seconds_retry(now, "last failed time")?;

    Ok(RedisScriptCall::new(
        RedisScript::Retry,
        vec![
            task_key(msg),
            keys::active_key(&msg.queue),
            keys::lease_key(&msg.queue),
            keys::retry_key(&msg.queue),
            keys::processed_key(&msg.queue, now),
            keys::failed_key(&msg.queue, now),
            keys::processed_total_key(&msg.queue),
            keys::failed_total_key(&msg.queue),
        ],
        vec![
            RedisArg::String(msg.id.clone()),
            encoded_msg_arg(&retry_message),
            RedisArg::I64(unix_seconds_retry(retry_at, "retry time")?),
            RedisArg::I64(retry_stats_expire_at(now)?),
            RedisArg::String(if is_failure { "1" } else { "0" }.to_owned()),
            RedisArg::I64(i64::MAX),
        ],
    ))
}

fn archive_stats_expire_at(now: SystemTime) -> Result<i64, RedisArchivePlanError> {
    let time = now
        .checked_add(STATS_TTL)
        .ok_or(RedisArchivePlanError::TimeOverflow("stats expiration"))?;
    unix_seconds_archive(time, "stats expiration")
}

fn retry_stats_expire_at(now: SystemTime) -> Result<i64, RedisRetryPlanError> {
    let time = now
        .checked_add(STATS_TTL)
        .ok_or(RedisRetryPlanError::TimeOverflow("stats expiration"))?;
    unix_seconds_retry(time, "stats expiration")
}

fn unix_seconds_archive(
    time: SystemTime,
    context: &'static str,
) -> Result<i64, RedisArchivePlanError> {
    let seconds = unix_seconds_floor(time);
    seconds
        .try_into()
        .map_err(|_| RedisArchivePlanError::TimeOverflow(context))
}

fn unix_seconds_retry(time: SystemTime, context: &'static str) -> Result<i64, RedisRetryPlanError> {
    let seconds = unix_seconds_floor(time);
    seconds
        .try_into()
        .map_err(|_| RedisRetryPlanError::TimeOverflow(context))
}
