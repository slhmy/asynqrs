use std::collections::HashMap;

use bytes::Bytes;

/// A unit of work to be performed.
///
/// Reference: Asynq v0.26.0 public `Task`, `NewTask`, and `NewTaskWithHeaders`
/// APIs: <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L22-L73>.
#[derive(Clone, PartialEq, Eq)]
pub struct Task {
    pub(super) type_name: String,
    pub(super) payload: Vec<u8>,
    pub(super) headers: HashMap<String, String>,
}

impl Task {
    pub fn new(type_name: impl Into<String>, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            type_name: type_name.into(),
            payload: payload.into(),
            headers: HashMap::new(),
        }
    }

    pub fn with_headers<I, K, V>(
        type_name: impl Into<String>,
        payload: impl Into<Vec<u8>>,
        headers: I,
    ) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        Self::new_with_headers(type_name, payload, headers)
    }

    /// Builds a task with headers and no task options.
    ///
    /// Reference: Asynq v0.26.0 public `NewTaskWithHeaders` builds a task with
    /// header metadata and optional enqueue options:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L67-L73>.
    pub fn new_with_headers<I, K, V>(
        type_name: impl Into<String>,
        payload: impl Into<Vec<u8>>,
        headers: I,
    ) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        Self {
            type_name: type_name.into(),
            payload: payload.into(),
            headers: headers
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        }
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    pub fn extend_headers<I, K, V>(mut self, headers: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.headers.extend(
            headers
                .into_iter()
                .map(|(key, value)| (key.into(), value.into())),
        );
        self
    }

    /// Returns the task type name.
    ///
    /// Reference: Asynq v0.26.0 public `Task.Type` accessor:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L38-L40>.
    pub fn task_type(&self) -> &str {
        &self.type_name
    }

    pub fn type_name(&self) -> &str {
        self.task_type()
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn payload_bytes(&self) -> Bytes {
        Bytes::copy_from_slice(&self.payload)
    }

    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }

    /// Returns mutable access to the task payload.
    ///
    /// Reference: Asynq v0.26.0 public `Task.Payload` returns the underlying
    /// Go byte slice, allowing callers to mutate payload bytes:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L38-L40>.
    pub fn payload_mut(&mut self) -> &mut Vec<u8> {
        &mut self.payload
    }

    pub fn headers(&self) -> &HashMap<String, String> {
        &self.headers
    }

    /// Returns mutable access to the task headers.
    ///
    /// Reference: Asynq v0.26.0 public `Task.Headers` returns the underlying
    /// Go map, allowing callers to mutate header entries:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L38-L40>.
    pub fn headers_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.headers
    }

    pub fn header(&self, key: &str) -> Option<&str> {
        self.headers.get(key).map(String::as_str)
    }

    pub fn insert_header(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.headers.insert(key.into(), value.into());
    }
}

impl std::fmt::Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Task")
            .field("type_name", &self.type_name)
            .field("payload", &self.payload)
            .field("headers", &self.headers)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ProcessingContext, ResultError, TaskType};
    use std::time::{Duration, SystemTime};

    #[test]
    fn creates_task_with_type_and_payload() {
        let task = Task::new("email:welcome", br#"{"user_id":42}"#.to_vec());

        assert_eq!(task.task_type(), "email:welcome");
        assert_eq!(task.type_name(), "email:welcome");
        assert_eq!(task.payload(), br#"{"user_id":42}"#);
        assert_eq!(
            task.payload_bytes(),
            bytes::Bytes::from_static(br#"{"user_id":42}"#)
        );
        assert!(task.headers().is_empty());
    }

    #[test]
    fn task_new_matches_constructor_shape() {
        let task = Task::new("email:welcome", br#"{"user_id":42}"#.to_vec());

        assert_eq!(task.task_type(), "email:welcome");
        assert_eq!(task.payload(), br#"{"user_id":42}"#);
        assert!(task.headers().is_empty());
    }

    #[test]
    fn creates_task_with_headers() {
        let task = Task::new_with_headers(
            "image:resize",
            b"payload".to_vec(),
            [("trace-id", "abc"), ("tenant", "acme")],
        );

        assert_eq!(task.type_name(), "image:resize");
        assert_eq!(task.payload(), b"payload");
        assert_eq!(task.header("trace-id"), Some("abc"));
        assert_eq!(task.header("tenant"), Some("acme"));
        assert_eq!(task.header("missing"), None);
    }

    #[test]
    fn inserts_headers() {
        let mut task = Task::new("email:welcome", Vec::new());

        task.insert_header("trace-id", "abc");
        task.insert_header("trace-id", "def");

        assert_eq!(task.header("trace-id"), Some("def"));
    }

    #[test]
    fn mutable_payload_and_headers_accessors_match_upstream_reference_semantics() {
        let mut task = Task::new("email:welcome", b"payload".to_vec());

        task.payload_mut()[0] = b'P';
        task.payload_mut().extend_from_slice(b"-v2");
        task.headers_mut()
            .insert("trace-id".to_owned(), "abc".to_owned());
        task.headers_mut()
            .insert("tenant".to_owned(), "acme".to_owned());

        assert_eq!(task.payload(), b"Payload-v2");
        assert_eq!(
            task.headers().get("trace-id").map(String::as_str),
            Some("abc")
        );
        assert_eq!(
            task.headers().get("tenant").map(String::as_str),
            Some("acme")
        );
    }

    #[test]
    fn fluent_task_builder_accepts_semantic_names() {
        let task = Task::new(TaskType::new("email:welcome"), b"payload".to_vec())
            .with_header("trace-id", "abc")
            .extend_headers([("tenant", "acme")]);

        assert_eq!(task.type_name(), "email:welcome");
        assert_eq!(task.header("trace-id"), Some("abc"));
        assert_eq!(task.header("tenant"), Some("acme"));
    }

    #[test]
    fn task_payload_can_be_taken_as_owned_bytes() {
        let task = Task::new("email:welcome", b"payload".to_vec());

        assert_eq!(task.into_payload(), b"payload");
    }

    #[test]
    fn processing_context_accessors_read_task_metadata() {
        let context = ProcessingContext::for_task(
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(30)),
            tokio_util::sync::CancellationToken::new(),
            "task-id".to_owned(),
            "critical".to_owned(),
            2,
            5,
        );

        assert_eq!(
            context.deadline(),
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(30))
        );
        assert!(!context.is_cancelled());
        assert_eq!(context.task_id(), "task-id");
        assert_eq!(context.queue_name(), "critical");
        assert_eq!(context.retry_count(), 2);
        assert_eq!(context.max_retry(), 5);
        assert!(context.result_writer().is_none());
        assert_eq!(
            context.write_result(b"result".to_vec()).unwrap_err(),
            ResultError::NoResultWriter
        );
    }

    #[tokio::test]
    async fn processing_context_without_result_writer_rejects_async_result_writes() {
        let context = ProcessingContext::for_task(
            None,
            tokio_util::sync::CancellationToken::new(),
            "task-id",
            "critical",
            2,
            5,
        );

        assert_eq!(
            context
                .write_result_async(b"result".to_vec())
                .await
                .unwrap_err(),
            ResultError::NoResultWriter
        );
    }
}
