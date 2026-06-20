pub(in crate::server) mod codec;
mod heartbeat;

use std::time::{Duration, SystemTime};

use prost::Message;
use uuid::Uuid;

use crate::pb;
use crate::server::{DEFAULT_SERVER_METADATA_TTL, QueueSelector, ServerError};

pub(super) use codec::server_info_i32;
pub(crate) use codec::worker_info_bytes;
pub(super) use heartbeat::{
    clear_server_metadata_ignoring_error, effective_metadata_heartbeat_interval,
    extend_active_worker_leases, metadata_heartbeat_ttl, run_metadata_heartbeat,
    write_server_metadata_ignoring_error,
};

/// Server heartbeat metadata written while the server loop is active.
///
/// Reference: Asynq v0.26.0 server heartbeat writes server info and worker
/// keys through `RDB.WriteServerState` and clears them on shutdown:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/heartbeat.go>.
///
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerMetadata {
    hostname: String,
    pid: i32,
    server_id: String,
    server_info: Vec<u8>,
    workers: Vec<Vec<u8>>,
    ttl: Duration,
}

impl ServerMetadata {
    pub fn new<I, W>(
        hostname: impl Into<String>,
        pid: i32,
        server_id: impl Into<String>,
        server_info: impl Into<Vec<u8>>,
        workers: I,
        ttl: Duration,
    ) -> Result<Self, ServerError>
    where
        I: IntoIterator<Item = W>,
        W: Into<String>,
    {
        let hostname = hostname.into();
        let server_id = server_id.into();
        if hostname.trim().is_empty() {
            return Err(ServerError::EmptyMetadataHostname);
        }
        if server_id.trim().is_empty() {
            return Err(ServerError::EmptyMetadataServerId);
        }
        let server_info = server_info.into();
        if server_info.is_empty() {
            return Err(ServerError::EmptyMetadataServerInfo);
        }
        if ttl.is_zero() {
            return Err(ServerError::ZeroMetadataTtl);
        }
        Ok(Self {
            hostname,
            pid,
            server_id,
            server_info,
            workers: workers
                .into_iter()
                .map(Into::into)
                .map(String::into_bytes)
                .collect(),
            ttl,
        })
    }

    pub fn for_current_process(queues: &[String]) -> Self {
        Self::for_current_process_parts(
            queues.iter().cloned().map(|queue| (queue, 1)).collect(),
            false,
            1,
        )
    }

    /// Builds default heartbeat metadata from the server queue selector.
    ///
    /// Reference: Asynq v0.26.0 `ServerInfo` preserves configured queue
    /// weights, strict-priority mode, and concurrency in heartbeat metadata:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/proto/asynq.proto#L97-L113>.
    pub fn for_current_process_with_queue_selector(
        queue_selector: &QueueSelector,
        concurrency: usize,
    ) -> Result<Self, ServerError> {
        if concurrency == 0 {
            return Err(ServerError::EmptyWorkerCount);
        }
        Ok(Self::for_current_process_parts(
            queue_selector.queue_priorities(),
            queue_selector.is_strict_priority(),
            server_info_i32(concurrency),
        ))
    }

    fn for_current_process_parts(
        queue_priorities: Vec<(String, i32)>,
        strict_priority: bool,
        concurrency: i32,
    ) -> Self {
        let hostname = current_hostname();
        let pid = codec::server_pid_i32(std::process::id());
        let server_id = Uuid::new_v4().to_string();
        // Reference: Asynq v0.26.0 heartbeat serializes only currently active
        // workers from its worker map; idle capacity is represented by
        // `ServerInfo.concurrency`, not placeholder `WorkerInfo` records:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/heartbeat.go#L129-L154>.
        // Runtime heartbeat writes populate active workers from server runtime
        // snapshots when tasks are in flight.
        let server_info = codec::server_info_bytes(
            &hostname,
            pid,
            &server_id,
            queue_priorities.clone(),
            strict_priority,
            concurrency,
            SystemTime::now(),
        );
        Self {
            hostname: hostname.clone(),
            pid,
            server_id: server_id.clone(),
            server_info,
            workers: Vec::new(),
            ttl: DEFAULT_SERVER_METADATA_TTL,
        }
    }

    pub fn hostname(&self) -> &str {
        &self.hostname
    }

    pub fn pid(&self) -> i32 {
        self.pid
    }

    pub fn server_id(&self) -> &str {
        &self.server_id
    }

    pub fn server_info(&self) -> &[u8] {
        &self.server_info
    }

    pub fn workers(&self) -> &[Vec<u8>] {
        &self.workers
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    pub(in crate::server) fn with_ttl(&self, ttl: Duration) -> Self {
        let mut metadata = self.clone();
        metadata.ttl = ttl;
        metadata
    }

    pub(crate) fn with_active_workers(&self, workers: Vec<Vec<u8>>) -> Self {
        let mut metadata = self.clone();
        if let Ok(mut info) = pb::asynq::ServerInfo::decode(metadata.server_info.as_slice()) {
            info.active_worker_count = server_info_i32(workers.len());
            metadata.server_info = info.encode_to_vec();
        }
        metadata.workers = workers;
        metadata
    }
}

fn current_hostname() -> String {
    // Reference: Asynq v0.26.0 `newHeartbeater` uses `os.Hostname` and falls
    // back to `unknown-host` when hostname lookup fails:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/heartbeat.go#L51-L54>.
    normalize_hostname(
        hostname::get()
            .ok()
            .and_then(|hostname| hostname.into_string().ok()),
    )
}

pub(in crate::server) fn normalize_hostname(hostname: Option<String>) -> String {
    hostname
        .filter(|hostname| !hostname.trim().is_empty())
        .unwrap_or_else(|| "unknown-host".to_owned())
}
