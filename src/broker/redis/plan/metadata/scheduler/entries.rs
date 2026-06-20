use std::time::{Duration, SystemTime};

use super::super::super::{RedisArg, RedisMetadataPlanError, RedisScript, RedisScriptCall};
use super::super::{metadata_duration_seconds, unix_seconds_metadata};
use crate::broker::redis::keys;

/// Redis command intent for writing scheduler entries metadata.
///
/// Reference: Asynq v0.26.0 `RDB.WriteSchedulerEntries`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisWriteSchedulerEntriesPlan {
    scheduler_entries_key: String,
    all_schedulers_key: String,
    expires_at: i64,
    call: RedisScriptCall,
}

/// Redis command intent for clearing scheduler entries metadata.
///
/// Reference: Asynq v0.26.0 `RDB.ClearSchedulerEntries`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisClearSchedulerEntriesPlan {
    entries_key: String,
}

/// Redis command intent for listing scheduler metadata keys.
///
/// Reference: Asynq v0.26.0 `RDB.ListSchedulerEntries` first removes expired
/// scheduler keys and returns live keys from `base.ALL_SCHEDULERS`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L2009-L2035>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisListSchedulerEntriesPlan {
    call: RedisScriptCall,
}

impl RedisWriteSchedulerEntriesPlan {
    pub fn from_entries<I>(
        scheduler_id: &str,
        entries: I,
        now: SystemTime,
        ttl: Duration,
    ) -> Result<Self, RedisMetadataPlanError>
    where
        I: IntoIterator<Item = (String, Vec<u8>)>,
    {
        let mut args = vec![RedisArg::I64(metadata_duration_seconds(
            ttl,
            "scheduler metadata ttl duration",
        )?)];
        // Reference: Asynq v0.26.0 `RDB.WriteSchedulerEntries` derives the
        // Redis scheduler key directly from the supplied scheduler id and
        // pushes each successfully encoded entry payload without validating
        // entry ids or encoded byte length:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1449-L1467>.
        for (_entry_id, entry) in entries {
            args.push(RedisArg::Bytes(entry));
        }
        let expires_at = unix_seconds_metadata(
            now.checked_add(ttl)
                .ok_or(RedisMetadataPlanError::TimeOverflow(
                    "scheduler metadata ttl",
                ))?,
            "scheduler metadata expiration",
        )?;
        let scheduler_entries_key = keys::scheduler_entries_key(scheduler_id);
        Ok(Self {
            scheduler_entries_key: scheduler_entries_key.clone(),
            all_schedulers_key: keys::ALL_SCHEDULERS.to_owned(),
            expires_at,
            call: RedisScriptCall::new(
                RedisScript::WriteSchedulerEntries,
                vec![scheduler_entries_key],
                args,
            ),
        })
    }

    pub fn scheduler_entries_key(&self) -> &str {
        &self.scheduler_entries_key
    }

    pub fn all_schedulers_key(&self) -> &str {
        &self.all_schedulers_key
    }

    pub fn expires_at(&self) -> i64 {
        self.expires_at
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}

impl RedisClearSchedulerEntriesPlan {
    pub fn from_scheduler(scheduler_id: &str) -> Result<Self, RedisMetadataPlanError> {
        // Reference: Asynq v0.26.0 `RDB.ClearSchedulerEntries` derives the
        // Redis scheduler key directly from the supplied scheduler id:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1470-L1481>.
        Ok(Self {
            entries_key: keys::scheduler_entries_key(scheduler_id),
        })
    }

    pub fn entries_key(&self) -> &str {
        &self.entries_key
    }

    pub fn all_schedulers_key(&self) -> &str {
        keys::ALL_SCHEDULERS
    }
}

impl RedisListSchedulerEntriesPlan {
    pub fn from_time(now: SystemTime) -> Result<Self, RedisMetadataPlanError> {
        Ok(Self {
            call: RedisScriptCall::new(
                RedisScript::ListSchedulerEntries,
                vec![keys::ALL_SCHEDULERS.to_owned()],
                vec![RedisArg::I64(unix_seconds_metadata(
                    now,
                    "scheduler metadata scan",
                )?)],
            ),
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}
