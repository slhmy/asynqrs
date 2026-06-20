//! Redis admin command plans for aggregation groups and aggregating tasks.
//!
//! Reference: Asynq v0.26.0 Inspector group and aggregating task commands:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go>.

use std::time::SystemTime;

use crate::Pagination;
use crate::broker::redis::keys;

use super::super::{
    ARCHIVED_EXPIRATION, MAX_ARCHIVE_SIZE, RedisAdminPlanError, RedisArg, RedisScript,
    RedisScriptCall,
};
use super::{pagination_start, pagination_stop, unix_seconds_admin};

/// Redis command intent for archiving all aggregating tasks in a queue group.
///
/// Reference: Asynq v0.26.0 `RDB.ArchiveAllAggregatingTasks`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1171-L1198>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisArchiveAllAggregatingTasksPlan {
    call: RedisScriptCall,
}

impl RedisArchiveAllAggregatingTasksPlan {
    pub fn from_queue_group_and_time(
        queue: &str,
        group: &str,
        now: SystemTime,
    ) -> Result<Self, RedisAdminPlanError> {
        if queue.trim().is_empty() {
            return Err(RedisAdminPlanError::EmptyQueueName);
        }
        let cutoff = now
            .checked_sub(ARCHIVED_EXPIRATION)
            .ok_or(RedisAdminPlanError::TimeOverflow("archive cutoff"))?;

        Ok(Self {
            call: RedisScriptCall::new(
                RedisScript::ArchiveAllAggregatingTasks,
                vec![
                    keys::group_key(queue, group),
                    keys::archived_key(queue),
                    keys::all_groups_key(queue),
                ],
                vec![
                    RedisArg::I64(unix_seconds_admin(now, "archive time")?),
                    RedisArg::I64(unix_seconds_admin(cutoff, "archive cutoff")?),
                    RedisArg::I64(MAX_ARCHIVE_SIZE as i64),
                    RedisArg::String(keys::task_key_prefix(queue)),
                    RedisArg::String(group.to_owned()),
                ],
            ),
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}

/// Redis command intent for deleting all aggregating tasks in a queue group.
///
/// Reference: Asynq v0.26.0 `RDB.DeleteAllAggregatingTasks`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1703-L1725>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisDeleteAllAggregatingTasksPlan {
    call: RedisScriptCall,
}

impl RedisDeleteAllAggregatingTasksPlan {
    pub fn from_queue_and_group(queue: &str, group: &str) -> Result<Self, RedisAdminPlanError> {
        if queue.trim().is_empty() {
            return Err(RedisAdminPlanError::EmptyQueueName);
        }

        Ok(Self {
            call: RedisScriptCall::new(
                RedisScript::DeleteAllAggregatingTasks,
                vec![keys::group_key(queue, group), keys::all_groups_key(queue)],
                vec![
                    RedisArg::String(keys::task_key_prefix(queue)),
                    RedisArg::String(group.to_owned()),
                ],
            ),
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisListAggregationGroupsPlan {
    key: String,
}

impl RedisListAggregationGroupsPlan {
    pub fn from_queue(queue: &str) -> Result<Self, RedisAdminPlanError> {
        // Reference: Asynq v0.26.0 `RDB.ListGroups` derives the all-groups
        // key directly from `qname`:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1022-L1030>.
        Ok(Self {
            key: keys::all_groups_key(queue),
        })
    }

    pub fn key(&self) -> &str {
        &self.key
    }
}

/// Redis command intent for listing aggregating task messages in a queue group.
///
/// Reference: Asynq v0.26.0 `RDB.ListAggregating`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L706-L721>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisListAggregatingTasksPlan {
    call: RedisScriptCall,
}

impl RedisListAggregatingTasksPlan {
    pub fn from_queue_group_and_pagination(
        queue: &str,
        group: &str,
        pagination: Pagination,
    ) -> Result<Self, RedisAdminPlanError> {
        if queue.trim().is_empty() {
            return Err(RedisAdminPlanError::EmptyQueueName);
        }

        Ok(Self {
            call: RedisScriptCall::new(
                RedisScript::ListTasks,
                vec![keys::group_key(queue, group)],
                vec![
                    RedisArg::I64(pagination_start(&pagination)?),
                    RedisArg::I64(pagination_stop(&pagination)?),
                    RedisArg::String(keys::task_key_prefix(queue)),
                    RedisArg::String("0".to_owned()),
                ],
            ),
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}

/// Redis command intent for moving all aggregating tasks in a group to pending.
///
/// Reference: Asynq v0.26.0 `RDB.RunAllAggregatingTasks`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1098-L1120>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisRunAllAggregatingTasksPlan {
    call: RedisScriptCall,
}

impl RedisRunAllAggregatingTasksPlan {
    pub fn from_queue_and_group(queue: &str, group: &str) -> Result<Self, RedisAdminPlanError> {
        if queue.trim().is_empty() {
            return Err(RedisAdminPlanError::EmptyQueueName);
        }

        Ok(Self {
            call: RedisScriptCall::new(
                RedisScript::RunAllAggregatingTasks,
                vec![
                    keys::group_key(queue, group),
                    keys::pending_key(queue),
                    keys::all_groups_key(queue),
                ],
                vec![
                    RedisArg::String(keys::task_key_prefix(queue)),
                    RedisArg::String(group.to_owned()),
                ],
            ),
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}
