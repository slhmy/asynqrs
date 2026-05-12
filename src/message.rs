use prost::Message;

use crate::{Task, pb};

pub use pb::asynq::TaskMessage;

#[derive(Debug)]
pub struct DecodeTaskMessageError(prost::DecodeError);

impl TaskMessage {
    pub fn from_task(task: &Task) -> Self {
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

    pub fn encode_to_vec(&self) -> Vec<u8> {
        Message::encode_to_vec(self)
    }

    pub fn decode_from_slice(data: &[u8]) -> Result<Self, DecodeTaskMessageError> {
        Self::decode(data).map_err(DecodeTaskMessageError)
    }
}

impl DecodeTaskMessageError {
    pub fn source(&self) -> &prost::DecodeError {
        &self.0
    }
}

impl std::fmt::Display for DecodeTaskMessageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "failed to decode task message: {}", self.0)
    }
}

impl std::error::Error for DecodeTaskMessageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

impl From<&Task> for TaskMessage {
    fn from(task: &Task) -> Self {
        Self::from_task(task)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        let encoded = msg.encode_to_vec();
        let decoded = TaskMessage::decode_from_slice(&encoded).unwrap();

        assert_eq!(decoded, msg);
    }
}
