use std::time::SystemTime;

use crate::EnqueuePlan;
use crate::broker::redis::keys;
use crate::task::TaskMessage;

use super::{unique_key, unique_ttl_seconds, unix_nanoseconds};
use crate::broker::redis::plan::{
    RedisArg, RedisEnqueuePlanError, RedisScript, RedisScriptCall, encoded_msg_arg, task_key,
};

/// Builds Redis script calls for immediate enqueue paths.
///
/// Reference: Asynq v0.26.0 enqueue and enqueue-unique scripts:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L6-L24>.
pub(super) fn pending_call(
    msg: &TaskMessage,
    now: SystemTime,
) -> Result<RedisScriptCall, RedisEnqueuePlanError> {
    Ok(RedisScriptCall::new(
        RedisScript::Enqueue,
        vec![task_key(msg), keys::pending_key(&msg.queue)],
        vec![
            encoded_msg_arg(msg),
            RedisArg::String(msg.id.clone()),
            RedisArg::I64(unix_nanoseconds(now)?),
        ],
    ))
}

pub(super) fn pending_unique_call(
    msg: &TaskMessage,
    plan: &EnqueuePlan,
    now: SystemTime,
) -> Result<RedisScriptCall, RedisEnqueuePlanError> {
    Ok(RedisScriptCall::new(
        RedisScript::EnqueueUnique,
        vec![
            unique_key(msg)?,
            task_key(msg),
            keys::pending_key(&msg.queue),
        ],
        vec![
            RedisArg::String(msg.id.clone()),
            RedisArg::I64(unique_ttl_seconds(plan)?),
            encoded_msg_arg(msg),
            RedisArg::I64(unix_nanoseconds(now)?),
        ],
    ))
}
