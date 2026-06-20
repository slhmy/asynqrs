use crate::EnqueuePlan;
use crate::broker::redis::keys;
use crate::task::TaskMessage;

use super::{scheduled_score, unique_key, unique_ttl_seconds};
use crate::broker::redis::plan::{
    RedisArg, RedisEnqueuePlanError, RedisScript, RedisScriptCall, encoded_msg_arg, task_key,
};

/// Builds Redis script calls for scheduled enqueue paths.
///
/// Reference: Asynq v0.26.0 schedule and schedule-unique scripts:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L6-L24>.
pub(super) fn scheduled_call(
    msg: &TaskMessage,
    plan: &EnqueuePlan,
) -> Result<RedisScriptCall, RedisEnqueuePlanError> {
    Ok(RedisScriptCall::new(
        RedisScript::Schedule,
        vec![task_key(msg), keys::scheduled_key(&msg.queue)],
        vec![
            encoded_msg_arg(msg),
            RedisArg::I64(scheduled_score(plan)?),
            RedisArg::String(msg.id.clone()),
        ],
    ))
}

pub(super) fn scheduled_unique_call(
    msg: &TaskMessage,
    plan: &EnqueuePlan,
) -> Result<RedisScriptCall, RedisEnqueuePlanError> {
    Ok(RedisScriptCall::new(
        RedisScript::ScheduleUnique,
        vec![
            unique_key(msg)?,
            task_key(msg),
            keys::scheduled_key(&msg.queue),
        ],
        vec![
            RedisArg::String(msg.id.clone()),
            RedisArg::I64(unique_ttl_seconds(plan)?),
            RedisArg::I64(scheduled_score(plan)?),
            encoded_msg_arg(msg),
        ],
    ))
}
