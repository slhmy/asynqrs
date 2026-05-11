use std::collections::HashMap;

/// A unit of work to be performed.
///
/// Reference: Asynq v0.26.0 public `Task`, `NewTask`, and `NewTaskWithHeaders`
/// APIs: <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L22-L73>.
pub struct Task {
    type_name: String,
    payload: Vec<u8>,
    headers: HashMap<String, String>,
    // TODO: Add task options once enqueue behavior is modeled.
    // Upstream stores `opts []Option` on Task and applies them when enqueuing.
    // TODO: Add a ResultWriter equivalent when worker-side task execution is modeled.
    // Upstream stores `w *ResultWriter` only for tasks passed to Handler.ProcessTask.
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
        Self {
            type_name: type_name.into(),
            payload: payload.into(),
            headers: headers
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        }
    }

    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn headers(&self) -> &HashMap<String, String> {
        &self.headers
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

impl Clone for Task {
    fn clone(&self) -> Self {
        Self {
            type_name: self.type_name.clone(),
            payload: self.payload.clone(),
            headers: self.headers.clone(),
        }
    }
}

impl PartialEq for Task {
    fn eq(&self, other: &Self) -> bool {
        self.type_name == other.type_name
            && self.payload == other.payload
            && self.headers == other.headers
    }
}

impl Eq for Task {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_task_with_type_and_payload() {
        let task = Task::new("email:welcome", br#"{"user_id":42}"#.to_vec());

        assert_eq!(task.type_name(), "email:welcome");
        assert_eq!(task.payload(), br#"{"user_id":42}"#);
        assert!(task.headers().is_empty());
    }

    #[test]
    fn creates_task_with_headers() {
        let task = Task::with_headers(
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
}
