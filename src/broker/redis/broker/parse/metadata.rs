use crate::{
    AdminError, SchedulerEnqueueEventInfo, SchedulerEntryInfo, ServerInfo, WorkerMetadataInfo,
    admin::{
        decode_scheduler_enqueue_event as codec_decode_scheduler_enqueue_event,
        decode_scheduler_entry as codec_decode_scheduler_entry,
        decode_server_info as codec_decode_server_info,
        decode_worker_info as codec_decode_worker_info,
    },
};

pub(in crate::broker::redis::broker) fn decode_server_info(
    data: Vec<u8>,
) -> Result<ServerInfo, AdminError> {
    // Reference: Asynq v0.26.0 decodes server protobuf metadata for
    // `RDB.ListServers`.
    // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L402-L424>.
    codec_decode_server_info(data.as_slice()).map_err(|error| AdminError::Other(error.to_string()))
}

pub(in crate::broker::redis::broker) fn decode_worker_info(
    data: Vec<u8>,
) -> Result<WorkerMetadataInfo, AdminError> {
    // Reference: Asynq v0.26.0 decodes worker protobuf metadata for
    // `RDB.ListWorkers`.
    // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L461-L481>.
    codec_decode_worker_info(data.as_slice()).map_err(|error| AdminError::Other(error.to_string()))
}

pub(in crate::broker::redis::broker) fn decode_scheduler_entry(
    data: Vec<u8>,
) -> Result<SchedulerEntryInfo, AdminError> {
    // Reference: Asynq v0.26.0 decodes scheduler entry protobuf metadata for
    // `Inspector.SchedulerEntries`.
    // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L527-L545>.
    codec_decode_scheduler_entry(data.as_slice())
        .map_err(|error| AdminError::Other(error.to_string()))
}

pub(in crate::broker::redis::broker) fn decode_scheduler_enqueue_event(
    data: Vec<u8>,
) -> Result<SchedulerEnqueueEventInfo, AdminError> {
    // Reference: Asynq v0.26.0 decodes scheduler enqueue event protobuf
    // metadata for `Inspector.ListSchedulerEnqueueEvents`.
    // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L570-L581>.
    codec_decode_scheduler_enqueue_event(data.as_slice())
        .map_err(|error| AdminError::Other(error.to_string()))
}
