use std::time::SystemTime;

use crate::broker::redis::keys;
use crate::task::TaskMessage;

use super::super::{
    RedisArg, RedisCompletePlanError, RedisScript, RedisScriptCall, STATS_TTL, encoded_msg_arg,
    task_key, unix_seconds_floor,
};

/// Redis command intent for marking an active task as successfully completed.
///
/// Reference: Asynq v0.26.0 `RDB.Done` and `RDB.MarkAsComplete`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L325-L379>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisCompletePlan {
    call: RedisScriptCall,
}

impl RedisCompletePlan {
    pub fn from_message(
        message: &TaskMessage,
        now: SystemTime,
    ) -> Result<Self, RedisCompletePlanError> {
        // Reference: Asynq v0.26.0 `RDB.Done` and `RDB.MarkAsComplete`
        // derive keys and script arguments directly from `msg.Queue` and
        // `msg.ID`:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L344-L473>.
        let call = if message.retention > 0 {
            mark_as_complete_call(message, now)?
        } else {
            done_call(message, now)?
        };
        Ok(Self { call })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}

fn done_call(
    msg: &TaskMessage,
    now: SystemTime,
) -> Result<RedisScriptCall, RedisCompletePlanError> {
    let script = if msg.unique_key.is_empty() {
        RedisScript::Done
    } else {
        RedisScript::DoneUnique
    };
    let mut keys = vec![
        keys::active_key(&msg.queue),
        keys::lease_key(&msg.queue),
        task_key(msg),
        keys::processed_key(&msg.queue, now),
        keys::processed_total_key(&msg.queue),
    ];
    if !msg.unique_key.is_empty() {
        keys.push(msg.unique_key.clone());
    }

    Ok(RedisScriptCall::new(
        script,
        keys,
        vec![
            RedisArg::String(msg.id.clone()),
            RedisArg::I64(stats_expire_at(now)?),
            RedisArg::I64(i64::MAX),
        ],
    ))
}

fn mark_as_complete_call(
    msg: &TaskMessage,
    now: SystemTime,
) -> Result<RedisScriptCall, RedisCompletePlanError> {
    let script = if msg.unique_key.is_empty() {
        RedisScript::MarkAsComplete
    } else {
        RedisScript::MarkAsCompleteUnique
    };
    let mut keys = vec![
        keys::active_key(&msg.queue),
        keys::lease_key(&msg.queue),
        keys::completed_key(&msg.queue),
        task_key(msg),
        keys::processed_key(&msg.queue, now),
        keys::processed_total_key(&msg.queue),
    ];
    if !msg.unique_key.is_empty() {
        keys.push(msg.unique_key.clone());
    }

    let completed_at = unix_seconds_complete(now, "complete time")?;
    // Reference: Asynq v0.26.0 `RDB.MarkAsComplete` computes
    // `now.Unix() + msg.Retention` with Go `int64` arithmetic:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L448-L468>.
    let expires_at = completed_at.wrapping_add(msg.retention);
    let mut completed = msg.clone();
    completed.completed_at = completed_at;

    Ok(RedisScriptCall::new(
        script,
        keys,
        vec![
            RedisArg::String(msg.id.clone()),
            RedisArg::I64(stats_expire_at(now)?),
            RedisArg::I64(expires_at),
            encoded_msg_arg(&completed),
            RedisArg::I64(i64::MAX),
        ],
    ))
}

fn stats_expire_at(now: SystemTime) -> Result<i64, RedisCompletePlanError> {
    let time = now
        .checked_add(STATS_TTL)
        .ok_or(RedisCompletePlanError::TimeOverflow("stats expiration"))?;
    unix_seconds_complete(time, "stats expiration")
}

fn unix_seconds_complete(
    time: SystemTime,
    context: &'static str,
) -> Result<i64, RedisCompletePlanError> {
    let seconds = unix_seconds_floor(time);
    seconds
        .try_into()
        .map_err(|_| RedisCompletePlanError::TimeOverflow(context))
}
