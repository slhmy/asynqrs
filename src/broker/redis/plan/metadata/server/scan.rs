use std::time::SystemTime;

use super::super::super::{RedisArg, RedisMetadataPlanError, RedisScript, RedisScriptCall};
use super::super::unix_seconds_metadata;
use crate::broker::redis::keys;

/// Redis command intent for listing live server metadata keys.
///
/// Reference: Asynq v0.26.0 `RDB.ListServers` discovers live server keys from
/// `base.ALL_SERVERS` and removes stale sorted-set members:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1933-L1964>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisListServerKeysPlan {
    call: RedisScriptCall,
}

/// Redis command intent for listing live worker metadata keys.
///
/// Reference: Asynq v0.26.0 `RDB.ListWorkers` discovers live worker keys from
/// `base.ALL_WORKERS` and removes stale sorted-set members:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1966-L2000>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisListWorkerKeysPlan {
    call: RedisScriptCall,
}

impl RedisListServerKeysPlan {
    pub fn from_time(now: SystemTime) -> Result<Self, RedisMetadataPlanError> {
        Ok(Self {
            call: RedisScriptCall::new(
                RedisScript::ListServerKeys,
                vec![keys::ALL_SERVERS.to_owned()],
                vec![RedisArg::I64(unix_seconds_metadata(
                    now,
                    "server metadata scan",
                )?)],
            ),
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}

impl RedisListWorkerKeysPlan {
    pub fn from_time(now: SystemTime) -> Result<Self, RedisMetadataPlanError> {
        Ok(Self {
            call: RedisScriptCall::new(
                RedisScript::ListWorkerKeys,
                vec![keys::ALL_WORKERS.to_owned()],
                vec![RedisArg::I64(unix_seconds_metadata(
                    now,
                    "worker metadata scan",
                )?)],
            ),
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}
