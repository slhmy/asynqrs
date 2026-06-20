use prost::Message;
use thiserror::Error;

use crate::{Task, pb};

use super::super::{SchedulerEnqueueEventInfo, SchedulerEntryInfo};
use super::time::{
    go_zero_time_timestamp, optional_scheduler_prev_time, system_time_from_protobuf_timestamp,
    system_time_to_protobuf_timestamp,
};

#[derive(Debug, Error)]
#[error("invalid scheduler entry: {0}")]
pub(crate) struct DecodeSchedulerEntryError(prost::DecodeError);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("cannot encode nil scheduler entry")]
pub(crate) struct EncodeSchedulerEntryError;

/// Encodes scheduler-entry metadata using Asynq's protobuf wire format.
///
/// Reference: Asynq v0.26.0 internal `base.encode_scheduler_entry`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L527-L545>.
/// Rust compatibility: `None` models upstream's nil scheduler-entry error
/// branch without exposing a raw pointer-like API.
pub(crate) fn encode_scheduler_entry(
    entry: Option<&SchedulerEntryInfo>,
) -> Result<Vec<u8>, EncodeSchedulerEntryError> {
    let entry = entry.ok_or(EncodeSchedulerEntryError)?;
    Ok(pb::asynq::SchedulerEntry {
        id: entry.id().to_owned(),
        spec: entry.spec().to_owned(),
        task_type: entry.task().type_name().to_owned(),
        task_payload: entry.task().payload().to_vec(),
        enqueue_options: entry.enqueue_options().to_vec(),
        next_enqueue_time: Some(system_time_to_protobuf_timestamp(entry.next_enqueue_at())),
        prev_enqueue_time: Some(
            entry
                .prev_enqueue_at()
                .map(system_time_to_protobuf_timestamp)
                .unwrap_or_else(go_zero_time_timestamp),
        ),
    }
    .encode_to_vec())
}

/// Decodes scheduler-entry metadata from Asynq's protobuf wire format.
///
/// Reference: Asynq v0.26.0 internal `base.decode_scheduler_entry`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L527-L545>.
pub(crate) fn decode_scheduler_entry(
    data: &[u8],
) -> Result<SchedulerEntryInfo, DecodeSchedulerEntryError> {
    let entry = pb::asynq::SchedulerEntry::decode(data).map_err(DecodeSchedulerEntryError)?;
    Ok(SchedulerEntryInfo::new(
        entry.id,
        entry.spec,
        Task::new(entry.task_type, entry.task_payload),
        entry.enqueue_options,
        system_time_from_protobuf_timestamp(entry.next_enqueue_time),
        optional_scheduler_prev_time(entry.prev_enqueue_time),
    ))
}

#[derive(Debug, Error)]
#[error("invalid scheduler enqueue event: {0}")]
pub(crate) struct DecodeSchedulerEnqueueEventError(prost::DecodeError);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("cannot encode nil enqueue event")]
pub(crate) struct EncodeSchedulerEnqueueEventError;

/// Encodes scheduler enqueue-event metadata using Asynq's protobuf wire format.
///
/// Reference: Asynq v0.26.0 internal `base.encode_scheduler_enqueue_event`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L559-L568>.
/// Rust compatibility: `None` models upstream's nil scheduler enqueue-event
/// error branch without exposing a raw pointer-like API.
pub(crate) fn encode_scheduler_enqueue_event(
    event: Option<&SchedulerEnqueueEventInfo>,
) -> Result<Vec<u8>, EncodeSchedulerEnqueueEventError> {
    let event = event.ok_or(EncodeSchedulerEnqueueEventError)?;
    Ok(pb::asynq::SchedulerEnqueueEvent {
        task_id: event.task_id().to_owned(),
        enqueue_time: Some(system_time_to_protobuf_timestamp(event.enqueued_at())),
    }
    .encode_to_vec())
}

/// Decodes scheduler enqueue-event metadata from Asynq's protobuf wire format.
///
/// Reference: Asynq v0.26.0 internal `base.decode_scheduler_enqueue_event`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L570-L581>.
pub(crate) fn decode_scheduler_enqueue_event(
    data: &[u8],
) -> Result<SchedulerEnqueueEventInfo, DecodeSchedulerEnqueueEventError> {
    let event =
        pb::asynq::SchedulerEnqueueEvent::decode(data).map_err(DecodeSchedulerEnqueueEventError)?;
    Ok(SchedulerEnqueueEventInfo::new(
        event.task_id,
        system_time_from_protobuf_timestamp(event.enqueue_time),
    ))
}
