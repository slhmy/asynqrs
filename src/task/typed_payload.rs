use thiserror::Error;

use super::Task;

/// Error returned while converting typed payloads to or from task payload bytes.
///
/// The variants intentionally carry strings instead of serde-specific error
/// types so the typed payload API remains usable without the `serde` feature.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TaskPayloadError {
    /// Payload encoding failed.
    #[error("failed to encode task payload: {0}")]
    Encode(String),
    /// Payload decoding failed.
    #[error("failed to decode task payload: {0}")]
    Decode(String),
    /// Typed task metadata did not contain a usable task type.
    #[error("invalid task type: {0}")]
    InvalidTaskType(String),
}

/// Typed payload conversion for Rust-native task definitions.
///
/// Rust design note: this trait is an optional ergonomic layer over
/// [`Task::new`]. It does not change Redis wire behavior; compatible payload
/// bytes still flow through the same task model.
pub trait TypedTaskPayload: Sized {
    /// Task type used when constructing an ordinary [`Task`].
    const TASK_TYPE: &'static str;

    /// Encodes this typed payload into the task payload bytes stored in Redis.
    fn encode_payload(self) -> Result<Vec<u8>, TaskPayloadError>;

    /// Decodes task payload bytes into the typed payload.
    fn decode_payload(bytes: &[u8]) -> Result<Self, TaskPayloadError>;

    /// Converts this typed payload into an ordinary [`Task`].
    fn into_task(self) -> Result<Task, TaskPayloadError> {
        validate_task_type(Self::TASK_TYPE)?;
        let payload = self.encode_payload()?;
        Ok(Task::new(Self::TASK_TYPE, payload))
    }
}

/// Validates typed task metadata before constructing a task.
pub fn validate_task_type(task_type: &str) -> Result<(), TaskPayloadError> {
    if task_type.trim().is_empty() {
        return Err(TaskPayloadError::InvalidTaskType(
            "task type must contain one or more non-whitespace characters".to_owned(),
        ));
    }

    Ok(())
}

/// Encodes a typed payload as JSON.
///
/// This helper is available with the `serde` feature and is used by
/// `#[derive(TaskPayload)]` when both `macros` and `serde` are enabled.
#[cfg(feature = "serde")]
pub fn encode_json_task_payload<T>(payload: &T) -> Result<Vec<u8>, TaskPayloadError>
where
    T: serde::Serialize,
{
    serde_json::to_vec(payload).map_err(|error| TaskPayloadError::Encode(error.to_string()))
}

/// Decodes a typed payload from JSON task payload bytes.
///
/// This helper is available with the `serde` feature and is used by
/// `#[derive(TaskPayload)]` when both `macros` and `serde` are enabled.
#[cfg(feature = "serde")]
pub fn decode_json_task_payload<T>(bytes: &[u8]) -> Result<T, TaskPayloadError>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_slice(bytes).map_err(|error| TaskPayloadError::Decode(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct PlainPayload;

    impl TypedTaskPayload for PlainPayload {
        const TASK_TYPE: &'static str = "plain:payload";

        fn encode_payload(self) -> Result<Vec<u8>, TaskPayloadError> {
            Ok(b"plain".to_vec())
        }

        fn decode_payload(bytes: &[u8]) -> Result<Self, TaskPayloadError> {
            if bytes == b"plain" {
                Ok(Self)
            } else {
                Err(TaskPayloadError::Decode(
                    "expected plain payload".to_owned(),
                ))
            }
        }
    }

    struct BlankPayload;

    impl TypedTaskPayload for BlankPayload {
        const TASK_TYPE: &'static str = " ";

        fn encode_payload(self) -> Result<Vec<u8>, TaskPayloadError> {
            Ok(Vec::new())
        }

        fn decode_payload(_bytes: &[u8]) -> Result<Self, TaskPayloadError> {
            Ok(Self)
        }
    }

    #[test]
    fn typed_payload_converts_into_task() {
        let task = PlainPayload.into_task().unwrap();

        assert_eq!(task.task_type(), "plain:payload");
        assert_eq!(task.payload(), b"plain");
    }

    #[test]
    fn typed_payload_rejects_blank_task_type() {
        let error = BlankPayload.into_task().unwrap_err();

        assert!(matches!(error, TaskPayloadError::InvalidTaskType(_)));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn json_payload_encode_failure_maps_to_payload_encode_error() {
        use serde::ser::{Serialize, Serializer};

        struct FailingSerialize;

        impl Serialize for FailingSerialize {
            fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                Err(serde::ser::Error::custom("synthetic encode failure"))
            }
        }

        let error = encode_json_task_payload(&FailingSerialize).unwrap_err();

        assert!(
            matches!(error, TaskPayloadError::Encode(message) if message.contains("synthetic encode failure"))
        );
    }

    #[cfg(feature = "serde")]
    #[test]
    fn json_payload_decode_failure_maps_to_payload_decode_error() {
        let error = decode_json_task_payload::<u64>(b"not-json").unwrap_err();

        assert!(matches!(error, TaskPayloadError::Decode(message) if !message.is_empty()));
    }

    #[cfg(all(feature = "macros", feature = "serde"))]
    #[test]
    fn derive_task_payload_round_trips_json_payload() {
        use crate::TaskPayload;

        #[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, TaskPayload)]
        #[task_type = "typed:payload"]
        struct DerivedPayload {
            id: u64,
        }

        let task = DerivedPayload { id: 42 }.into_task().unwrap();

        assert_eq!(task.task_type(), "typed:payload");
        assert_eq!(
            DerivedPayload::decode_payload(task.payload()).unwrap(),
            DerivedPayload { id: 42 }
        );
    }
}
