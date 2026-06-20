pub(crate) use pb::asynq::TaskMessage;

use crate::Task;
use crate::pb;
use prost::Message;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[derive(Debug, Error)]
#[error("failed to decode task message: {0}")]
pub(crate) struct DecodeTaskMessageError(prost::DecodeError);

impl TaskMessage {
    pub(crate) fn from_task(task: &Task) -> Self {
        Self {
            r#type: task.type_name().to_owned(),
            payload: task.payload().to_vec(),
            headers: task.headers().clone(),
            id: String::new(),
            queue: String::new(),
            retry: 0,
            retried: 0,
            error_msg: String::new(),
            last_failed_at: 0,
            timeout: 0,
            deadline: 0,
            unique_key: String::new(),
            group_key: String::new(),
            retention: 0,
            completed_at: 0,
        }
    }

    pub(crate) fn encode_to_vec(&self) -> Vec<u8> {
        Message::encode_to_vec(self)
    }

    pub(crate) fn decode_from_slice(data: &[u8]) -> Result<Self, DecodeTaskMessageError> {
        Self::decode(data).map_err(DecodeTaskMessageError)
    }
}

impl From<&Task> for TaskMessage {
    fn from(task: &Task) -> Self {
        Self::from_task(task)
    }
}

pub(crate) fn duration_seconds(duration: Duration) -> i64 {
    duration.as_secs().try_into().unwrap_or(i64::MAX)
}

pub(crate) fn retry_count_i32(retry: u64) -> i32 {
    // Reference: Asynq v0.26.0 stores retry counts in `base.TaskMessage.Retry`
    // as Go `int`, then encodes them into the protobuf `int32` field with a
    // direct `int32(msg.Retry)` conversion:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L179-L186>.
    retry as i32
}

pub(crate) fn unix_seconds(time: SystemTime) -> i64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration_seconds(duration),
        Err(error) => {
            let duration = error.duration();
            let seconds = duration_seconds(duration);
            if duration.subsec_nanos() == 0 {
                -seconds
            } else {
                -seconds.saturating_add(1)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Task;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
    #[error("cannot encode nil message")]
    struct EncodeTaskMessageError;

    /// Encodes a task message using Asynq's protobuf wire format.
    ///
    /// Reference: Asynq v0.26.0 internal `base.EncodeMessage`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L269-L291>.
    /// Rust compatibility: `None` models upstream's nil message error branch
    /// without exposing a raw pointer-like API.
    fn encode_message(message: Option<&TaskMessage>) -> Result<Vec<u8>, EncodeTaskMessageError> {
        message
            .map(TaskMessage::encode_to_vec)
            .ok_or(EncodeTaskMessageError)
    }

    /// Decodes a task message from Asynq's protobuf wire format.
    ///
    /// Reference: Asynq v0.26.0 internal `base.DecodeMessage`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L293-L317>.
    fn decode_message(data: &[u8]) -> Result<TaskMessage, DecodeTaskMessageError> {
        TaskMessage::decode_from_slice(data)
    }

    #[test]
    fn builds_message_from_task() {
        let task = Task::with_headers("email:welcome", b"{}".to_vec(), [("trace-id", "abc")]);

        let msg = TaskMessage::from_task(&task);

        assert_eq!(msg.r#type, "email:welcome");
        assert_eq!(msg.payload, b"{}");
        assert_eq!(msg.headers.get("trace-id").map(String::as_str), Some("abc"));
        assert_eq!(msg.retry, 0);
        assert_eq!(msg.retried, 0);
    }

    #[test]
    fn encodes_and_decodes_message() {
        let mut msg = TaskMessage::from_task(&Task::new("image:resize", b"payload".to_vec()));
        msg.id = "task-id".to_owned();
        msg.queue = "default".to_owned();
        msg.retry = 25;
        msg.timeout = 30;

        let encoded = encode_message(Some(&msg)).unwrap();
        let decoded = decode_message(&encoded).unwrap();

        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_message_rejects_none_like_upstream_nil_message() {
        let error = encode_message(None).unwrap_err();

        assert_eq!(error, EncodeTaskMessageError);
        assert_eq!(error.to_string(), "cannot encode nil message");
    }
}
