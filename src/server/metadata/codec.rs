use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{ServerInfo, WorkerMetadataInfo};
use crate::{
    admin::{ServerInfoParts, WorkerMetadataInfoParts, encode_server_info, encode_worker_info},
    pb,
};

pub(crate) fn worker_info_bytes(
    hostname: &str,
    pid: i32,
    server_id: &str,
    message: &pb::asynq::TaskMessage,
    start_time: SystemTime,
    deadline: Option<SystemTime>,
) -> Vec<u8> {
    // Reference: Asynq v0.26.0 heartbeat serializes currently active workers
    // as protobuf `WorkerInfo` values and stores them by task id:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/heartbeat.go#L129-L154>.
    let info = WorkerMetadataInfo::new(WorkerMetadataInfoParts {
        host: hostname.to_owned(),
        pid,
        server_id: server_id.to_owned(),
        task_id: message.id.clone(),
        task_type: message.r#type.clone(),
        task_payload: message.payload.clone(),
        queue: message.queue.clone(),
        started: start_time,
        deadline: deadline.unwrap_or_else(go_zero_time),
    });
    encode_worker_info(Some(&info)).expect("non-nil worker info encodes")
}

pub(super) fn server_info_bytes(
    hostname: &str,
    pid: i32,
    server_id: &str,
    queue_priorities: Vec<(String, i32)>,
    strict_priority: bool,
    concurrency: i32,
    start_time: SystemTime,
) -> Vec<u8> {
    // Reference: Asynq v0.26.0 heartbeat stores protobuf `ServerInfo` values
    // through `RDB.WriteServerState`.
    let info = ServerInfo::new(ServerInfoParts {
        id: server_id.to_owned(),
        host: hostname.to_owned(),
        pid,
        concurrency,
        queues: queue_priorities.into_iter().collect(),
        strict_priority,
        started: start_time,
        status: "active".to_owned(),
        active_workers: Vec::new(),
    });
    encode_server_info(Some(&info)).expect("non-nil server info encodes")
}

pub(in crate::server) fn server_info_i32(value: usize) -> i32 {
    // Reference: Asynq v0.26.0 `encode_server_info` writes Go `int` fields to
    // protobuf `int32` fields using direct `int32(...)` conversions for queue
    // priorities, concurrency, and active worker counts:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L328-L342>.
    value as i32
}

pub(in crate::server) fn server_pid_i32(pid: u32) -> i32 {
    // Reference: Asynq v0.26.0 `encode_server_info` and `encode_worker_info` write
    // Go `int` process ids to protobuf `int32` fields using direct `int32(...)`
    // conversions:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L378-L399>.
    // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L440-L458>.
    pid as i32
}

fn go_zero_time() -> SystemTime {
    // Reference: Asynq v0.26.0 `encode_worker_info` encodes an unset deadline as
    // Go's zero `time.Time{}` via `timestamppb.New`.
    // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L440-L458>.
    UNIX_EPOCH - Duration::from_secs(62_135_596_800)
}
