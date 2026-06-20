use std::time::SystemTime;

use crate::EnqueuePlan;
use crate::broker::redis::keys;
use crate::task::TaskMessage;
use crate::task::unix_seconds;

use super::{group_key, unique_key, unique_ttl_seconds};
use crate::broker::redis::plan::{
    RedisArg, RedisEnqueuePlanError, RedisScript, RedisScriptCall, encoded_msg_arg, task_key,
};

/// Builds Redis script calls for group aggregation enqueue paths.
///
/// Reference: Asynq v0.26.0 aggregation enqueue scripts:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L6-L24>.
pub(super) fn group_call(
    msg: &TaskMessage,
    now: SystemTime,
) -> Result<RedisScriptCall, RedisEnqueuePlanError> {
    let group = group_key(msg)?;
    Ok(RedisScriptCall::new(
        RedisScript::AddToGroup,
        vec![
            task_key(msg),
            keys::group_key(&msg.queue, group),
            keys::all_groups_key(&msg.queue),
        ],
        vec![
            encoded_msg_arg(msg),
            RedisArg::String(msg.id.clone()),
            RedisArg::I64(unix_seconds(now)),
            RedisArg::String(group.to_owned()),
        ],
    ))
}

pub(super) fn group_unique_call(
    msg: &TaskMessage,
    plan: &EnqueuePlan,
    now: SystemTime,
) -> Result<RedisScriptCall, RedisEnqueuePlanError> {
    let group = group_key(msg)?;
    Ok(RedisScriptCall::new(
        RedisScript::AddToGroupUnique,
        vec![
            task_key(msg),
            keys::group_key(&msg.queue, group),
            keys::all_groups_key(&msg.queue),
            unique_key(msg)?,
        ],
        vec![
            encoded_msg_arg(msg),
            RedisArg::String(msg.id.clone()),
            RedisArg::I64(unix_seconds(now)),
            RedisArg::String(group.to_owned()),
            RedisArg::I64(unique_ttl_seconds(plan)?),
        ],
    ))
}
