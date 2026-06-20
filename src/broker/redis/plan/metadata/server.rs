use std::time::{Duration, SystemTime};

use super::super::{RedisArg, RedisMetadataPlanError, RedisScript, RedisScriptCall};
use super::{metadata_duration_seconds, unix_seconds_metadata, worker_info_id};
use crate::broker::redis::keys;

mod scan;

pub use scan::{RedisListServerKeysPlan, RedisListWorkerKeysPlan};

/// Redis command intent for writing server heartbeat metadata.
///
/// Reference: Asynq v0.26.0 `RDB.WriteServerState`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisWriteServerStatePlan {
    call: RedisScriptCall,
    server_info_key: String,
    workers_key: String,
    all_servers_key: String,
    all_workers_key: String,
    expires_at: i64,
}

/// Redis command intent for clearing server heartbeat metadata.
///
/// Reference: Asynq v0.26.0 `RDB.ClearServerState`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisClearServerStatePlan {
    server_info_key: String,
    workers_key: String,
    all_servers_key: String,
    all_workers_key: String,
    call: RedisScriptCall,
}

impl RedisWriteServerStatePlan {
    pub fn from_server(
        hostname: &str,
        pid: i32,
        server_id: &str,
        server_info: impl Into<Vec<u8>>,
        workers: impl IntoIterator<Item = Vec<u8>>,
        now: SystemTime,
        ttl: Duration,
    ) -> Result<Self, RedisMetadataPlanError> {
        let server_info = server_info.into();
        let ttl_seconds = metadata_duration_seconds(ttl, "server metadata ttl duration")?;
        let mut args = vec![RedisArg::I64(ttl_seconds), RedisArg::Bytes(server_info)];
        for worker in workers {
            let Some(worker_id) = worker_info_id(&worker) else {
                continue;
            };
            args.push(RedisArg::String(worker_id));
            args.push(RedisArg::Bytes(worker));
        }
        let expires_at = unix_seconds_metadata(
            now.checked_add(ttl)
                .ok_or(RedisMetadataPlanError::TimeOverflow("server metadata ttl"))?,
            "server metadata expiration",
        )?;
        // Reference: Asynq v0.26.0 `RDB.WriteServerState` derives server and
        // worker keys directly from `info.Host`, `info.PID`, and
        // `info.ServerID`, and passes the encoded server info through:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1388-L1413>.
        let server_info_key = keys::server_info_key(hostname, pid, server_id);
        let workers_key = keys::workers_key(hostname, pid, server_id);
        Ok(Self {
            server_info_key: server_info_key.clone(),
            workers_key: workers_key.clone(),
            all_servers_key: keys::ALL_SERVERS.to_owned(),
            all_workers_key: keys::ALL_WORKERS.to_owned(),
            expires_at,
            call: RedisScriptCall::new(
                RedisScript::WriteServerState,
                vec![server_info_key, workers_key],
                args,
            ),
        })
    }

    pub fn server_info_key(&self) -> &str {
        &self.server_info_key
    }

    pub fn workers_key(&self) -> &str {
        &self.workers_key
    }

    pub fn all_servers_key(&self) -> &str {
        &self.all_servers_key
    }

    pub fn all_workers_key(&self) -> &str {
        &self.all_workers_key
    }

    pub fn expires_at(&self) -> i64 {
        self.expires_at
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}

impl RedisClearServerStatePlan {
    pub fn from_server(
        hostname: &str,
        pid: i32,
        server_id: &str,
    ) -> Result<Self, RedisMetadataPlanError> {
        // Reference: Asynq v0.26.0 `RDB.ClearServerState` derives server and
        // worker keys directly from the supplied host, pid, and server id:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1423-L1435>.
        let server_info_key = keys::server_info_key(hostname, pid, server_id);
        let workers_key = keys::workers_key(hostname, pid, server_id);
        Ok(Self {
            server_info_key: server_info_key.clone(),
            workers_key: workers_key.clone(),
            all_servers_key: keys::ALL_SERVERS.to_owned(),
            all_workers_key: keys::ALL_WORKERS.to_owned(),
            call: RedisScriptCall::new(
                RedisScript::ClearServerState,
                vec![server_info_key, workers_key],
                Vec::new(),
            ),
        })
    }

    pub fn server_info_key(&self) -> &str {
        &self.server_info_key
    }

    pub fn workers_key(&self) -> &str {
        &self.workers_key
    }

    pub fn all_servers_key(&self) -> &str {
        &self.all_servers_key
    }

    pub fn all_workers_key(&self) -> &str {
        &self.all_workers_key
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}
