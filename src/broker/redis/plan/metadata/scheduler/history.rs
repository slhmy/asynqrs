use std::time::SystemTime;

use super::super::super::{RedisArg, RedisMetadataPlanError, RedisScript, RedisScriptCall};
use super::super::unix_seconds_metadata;
use crate::Pagination;
use crate::broker::redis::keys;

/// Redis command intent for listing scheduler enqueue events.
///
/// Reference: Asynq v0.26.0 `RDB.ListSchedulerEnqueueEvents`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L2037-L2057>.
/// Reference: Asynq v0.26.0 delegates directly to Redis using the derived
/// scheduler history key, without a local empty entry-id precheck.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisListSchedulerEnqueueEventsPlan {
    history_key: String,
    pagination: Pagination,
}

/// Redis command intent for recording a scheduler enqueue event.
///
/// Reference: Asynq v0.26.0 `RDB.RecordSchedulerEnqueueEvent`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1511-L1535>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisRecordSchedulerEnqueueEventPlan {
    call: RedisScriptCall,
}

/// Redis command intent for clearing scheduler history metadata.
///
/// Reference: Asynq v0.26.0 `RDB.ClearSchedulerHistory`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1538-L1546>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisClearSchedulerHistoryPlan {
    history_key: String,
}

impl RedisListSchedulerEnqueueEventsPlan {
    pub fn from_entry_and_pagination(
        entry_id: &str,
        pagination: Pagination,
    ) -> Result<Self, RedisMetadataPlanError> {
        Ok(Self {
            history_key: keys::scheduler_history_key(entry_id),
            pagination,
        })
    }

    pub fn history_key(&self) -> &str {
        &self.history_key
    }

    pub fn pagination(&self) -> Pagination {
        self.pagination
    }
}

impl RedisRecordSchedulerEnqueueEventPlan {
    const MAX_EVENTS: i64 = 1000;

    pub fn from_event(
        entry_id: &str,
        event: impl Into<Vec<u8>>,
        now: SystemTime,
    ) -> Result<Self, RedisMetadataPlanError> {
        let event = event.into();
        // Reference: Asynq v0.26.0 `RDB.RecordSchedulerEnqueueEvent` derives
        // the Redis history key from the supplied entry id and passes the
        // encoded event bytes directly to the script:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1519-L1535>.
        let history_key = keys::scheduler_history_key(entry_id);
        Ok(Self {
            call: RedisScriptCall::new(
                RedisScript::RecordSchedulerEnqueueEvent,
                vec![history_key],
                vec![
                    RedisArg::I64(unix_seconds_metadata(now, "scheduler enqueue event")?),
                    RedisArg::Bytes(event),
                    RedisArg::I64(Self::MAX_EVENTS),
                ],
            ),
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}

impl RedisClearSchedulerHistoryPlan {
    pub fn from_entry(entry_id: &str) -> Result<Self, RedisMetadataPlanError> {
        Ok(Self {
            history_key: keys::scheduler_history_key(entry_id),
        })
    }

    pub fn history_key(&self) -> &str {
        &self.history_key
    }
}
