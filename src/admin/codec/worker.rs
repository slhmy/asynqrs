use prost::Message;
use thiserror::Error;

use crate::pb;

use super::super::{WorkerMetadataInfo, WorkerMetadataInfoParts};
use super::time::{
    optional_system_time_from_protobuf_timestamp, system_time_to_protobuf_timestamp,
};

#[derive(Debug, Error)]
#[error("invalid worker info: {0}")]
pub(crate) struct DecodeWorkerInfoError(prost::DecodeError);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("cannot encode nil worker info")]
pub(crate) struct EncodeWorkerInfoError;

/// Encodes worker metadata using Asynq's protobuf wire format.
///
/// Reference: Asynq v0.26.0 internal `base.encode_worker_info`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L440-L458>.
/// Rust compatibility: `None` models upstream's nil worker-info error branch
/// without exposing a raw pointer-like API.
pub(crate) fn encode_worker_info(
    info: Option<&WorkerMetadataInfo>,
) -> Result<Vec<u8>, EncodeWorkerInfoError> {
    let info = info.ok_or(EncodeWorkerInfoError)?;
    Ok(pb::asynq::WorkerInfo {
        host: info.host().to_owned(),
        pid: info.pid(),
        server_id: info.server_id().to_owned(),
        task_id: info.task_id().to_owned(),
        task_type: info.task_type().to_owned(),
        task_payload: info.task_payload().to_vec(),
        queue: info.queue().to_owned(),
        start_time: Some(system_time_to_protobuf_timestamp(info.started())),
        deadline: Some(system_time_to_protobuf_timestamp(info.deadline())),
    }
    .encode_to_vec())
}

/// Decodes worker metadata from Asynq's protobuf wire format.
///
/// Reference: Asynq v0.26.0 internal `base.decode_worker_info`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L461-L481>.
pub(crate) fn decode_worker_info(data: &[u8]) -> Result<WorkerMetadataInfo, DecodeWorkerInfoError> {
    let info = pb::asynq::WorkerInfo::decode(data).map_err(DecodeWorkerInfoError)?;
    Ok(WorkerMetadataInfo::new(WorkerMetadataInfoParts {
        host: info.host,
        pid: info.pid,
        server_id: info.server_id,
        task_id: info.task_id,
        task_type: info.task_type,
        task_payload: info.task_payload,
        queue: info.queue,
        started: optional_system_time_from_protobuf_timestamp(info.start_time),
        deadline: optional_system_time_from_protobuf_timestamp(info.deadline),
    }))
}
