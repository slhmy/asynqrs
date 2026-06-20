//! Redis task lifecycle command plans.
//!
//! Reference: Asynq v0.26.0 task lifecycle Redis commands:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L325-L507>.

mod complete;
mod failure;
mod lease;
mod maintenance;

pub use complete::RedisCompletePlan;
pub use failure::{RedisArchivePlan, RedisRetryPlan};
pub use lease::{RedisExtendLeasePlan, RedisRecoverPlan};
pub use maintenance::{RedisDeleteExpiredCompletedTasksPlan, RedisForwardPlan};

use crate::broker::redis::keys;
use crate::task::TaskMessage;

use super::{
    RedisArg, RedisRequeuePlanError, RedisScript, RedisScriptCall, RedisWriteResultPlanError,
    task_key,
};

/// Redis command intent for returning an active task back to pending.
///
/// Reference: Asynq v0.26.0 `RDB.Requeue`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L486-L506>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisRequeuePlan {
    call: RedisScriptCall,
}

impl RedisRequeuePlan {
    pub fn from_message(message: &TaskMessage) -> Result<Self, RedisRequeuePlanError> {
        // Reference: Asynq v0.26.0 `RDB.Requeue` derives keys and script
        // arguments directly from `msg.Queue` and `msg.ID`:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L497-L507>.
        Ok(Self {
            call: RedisScriptCall::new(
                RedisScript::Requeue,
                vec![
                    keys::active_key(&message.queue),
                    keys::lease_key(&message.queue),
                    keys::pending_key(&message.queue),
                    task_key(message),
                ],
                vec![RedisArg::String(message.id.clone())],
            ),
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}

/// Redis command intent for writing task result bytes.
///
/// Reference: Asynq v0.26.0 `RDB.WriteResult`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisWriteResultPlan {
    key: String,
    data: Vec<u8>,
}

impl RedisWriteResultPlan {
    pub fn from_task(
        queue: &str,
        task_id: &str,
        data: impl Into<Vec<u8>>,
    ) -> Result<Self, RedisWriteResultPlanError> {
        // Reference: Asynq v0.26.0 `RDB.WriteResult` derives the task key
        // directly from the supplied queue and task id:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1549-L1558>.
        Ok(Self {
            key: keys::task_key(queue, task_id),
            data: data.into(),
        })
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn field(&self) -> &str {
        "result"
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }
}
