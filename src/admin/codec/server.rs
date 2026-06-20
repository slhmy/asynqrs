use prost::Message;
use thiserror::Error;

use crate::pb;

use super::super::{ServerInfo, ServerInfoParts};
use super::time::{
    optional_system_time_from_protobuf_timestamp, system_time_to_protobuf_timestamp,
};

#[derive(Debug, Error)]
#[error("invalid server info: {0}")]
pub(crate) struct DecodeServerInfoError(prost::DecodeError);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("cannot encode nil server info")]
pub(crate) struct EncodeServerInfoError;

/// Encodes server metadata using Asynq's protobuf wire format.
///
/// Reference: Asynq v0.26.0 internal `base.encode_server_info`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L378-L400>.
/// Rust compatibility: `None` models upstream's nil server-info error branch
/// without exposing a raw pointer-like API.
pub(crate) fn encode_server_info(
    info: Option<&ServerInfo>,
) -> Result<Vec<u8>, EncodeServerInfoError> {
    let info = info.ok_or(EncodeServerInfoError)?;
    Ok(pb::asynq::ServerInfo {
        host: info.host().to_owned(),
        pid: info.pid(),
        server_id: info.id().to_owned(),
        concurrency: info.concurrency(),
        queues: info.queues().clone(),
        strict_priority: info.strict_priority(),
        status: info.status().to_owned(),
        start_time: Some(system_time_to_protobuf_timestamp(info.started())),
        active_worker_count: info.active_workers().len() as i32,
    }
    .encode_to_vec())
}

/// Decodes server metadata from Asynq's protobuf wire format.
///
/// Reference: Asynq v0.26.0 internal `base.decode_server_info`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L402-L424>.
pub(crate) fn decode_server_info(data: &[u8]) -> Result<ServerInfo, DecodeServerInfoError> {
    let info = pb::asynq::ServerInfo::decode(data).map_err(DecodeServerInfoError)?;
    Ok(ServerInfo::new(ServerInfoParts {
        id: info.server_id,
        host: info.host,
        pid: info.pid,
        concurrency: info.concurrency,
        queues: info.queues,
        strict_priority: info.strict_priority,
        started: optional_system_time_from_protobuf_timestamp(info.start_time),
        status: info.status,
        active_workers: Vec::new(),
    }))
}
