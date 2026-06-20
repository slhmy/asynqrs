//! Redis enqueue command plans.
//!
//! Reference: Asynq v0.26.0 Redis enqueue scripts and RDB enqueue methods:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L6-L24>.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::broker::redis::keys;
use crate::task::TaskMessage;
use crate::task::{duration_seconds, unix_seconds};
use crate::{EnqueuePlan, TaskState};

use super::{RedisEnqueueOperation, RedisEnqueuePlanError, RedisScriptCall, duration_nanoseconds};

mod group;
mod pending;
mod scheduled;

/// Redis command intent for enqueueing a task.
///
/// Reference: Asynq v0.26.0 Redis enqueue scripts and RDB enqueue methods:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L6-L24>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisEnqueuePlan {
    operations: Vec<RedisEnqueueOperation>,
}

impl RedisEnqueuePlan {
    pub fn from_enqueue_plan(
        plan: &EnqueuePlan,
        now: SystemTime,
    ) -> Result<Self, RedisEnqueuePlanError> {
        let msg = plan.message();
        let queue = msg.queue.clone();
        let mut operations = vec![RedisEnqueueOperation::PublishQueue {
            key: keys::ALL_QUEUES.to_owned(),
            queue: queue.clone(),
        }];
        operations.push(RedisEnqueueOperation::EvalScript(script_call(plan, now)?));
        Ok(Self { operations })
    }

    pub fn operations(&self) -> &[RedisEnqueueOperation] {
        &self.operations
    }
}

fn script_call(
    plan: &EnqueuePlan,
    now: SystemTime,
) -> Result<RedisScriptCall, RedisEnqueuePlanError> {
    let msg = plan.message();
    let unique = !msg.unique_key.is_empty();
    match (plan.state(), unique) {
        (TaskState::Pending, false) => pending::pending_call(msg, now),
        (TaskState::Pending, true) => pending::pending_unique_call(msg, plan, now),
        (TaskState::Scheduled, false) => scheduled::scheduled_call(msg, plan),
        (TaskState::Scheduled, true) => scheduled::scheduled_unique_call(msg, plan),
        (TaskState::Aggregating, false) => group::group_call(msg, now),
        (TaskState::Aggregating, true) => group::group_unique_call(msg, plan, now),
        (state, _) => Err(RedisEnqueuePlanError::UnsupportedState(state)),
    }
}

pub(super) fn unique_key(msg: &TaskMessage) -> Result<String, RedisEnqueuePlanError> {
    if msg.unique_key.is_empty() {
        return Err(RedisEnqueuePlanError::MissingUniqueKey);
    }
    Ok(msg.unique_key.clone())
}

pub(super) fn group_key(msg: &TaskMessage) -> Result<&str, RedisEnqueuePlanError> {
    if msg.group_key.is_empty() {
        return Err(RedisEnqueuePlanError::MissingGroupKey);
    }
    Ok(&msg.group_key)
}

pub(super) fn unique_ttl_seconds(plan: &EnqueuePlan) -> Result<i64, RedisEnqueuePlanError> {
    plan.unique_lock_ttl()
        .map(duration_seconds)
        .ok_or(RedisEnqueuePlanError::MissingUniqueLockTtl)
}

pub(super) fn scheduled_score(plan: &EnqueuePlan) -> Result<i64, RedisEnqueuePlanError> {
    plan.next_process_at()
        .map(unix_seconds)
        .ok_or(RedisEnqueuePlanError::MissingScheduledProcessTime)
}

pub(super) fn unix_nanoseconds(time: SystemTime) -> Result<i64, RedisEnqueuePlanError> {
    let nanos = match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration_nanoseconds(duration),
        Err(error) => -duration_nanoseconds(error.duration()),
    };
    nanos
        .try_into()
        .map_err(|_| RedisEnqueuePlanError::TimeOverflow("unix nanoseconds"))
}
