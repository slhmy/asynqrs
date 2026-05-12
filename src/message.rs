use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prost::Message;

use crate::{Task, TaskOption, pb};

pub use pb::asynq::TaskMessage;

#[derive(Debug)]
pub struct DecodeTaskMessageError(prost::DecodeError);

impl TaskMessage {
    pub fn from_task(task: &Task) -> Self {
        let mut msg = Self {
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
        };
        msg.apply_options(task.options());
        msg
    }

    pub fn encode_to_vec(&self) -> Vec<u8> {
        Message::encode_to_vec(self)
    }

    pub fn decode_from_slice(data: &[u8]) -> Result<Self, DecodeTaskMessageError> {
        Self::decode(data).map_err(DecodeTaskMessageError)
    }

    /// Reference: Asynq v0.26.0 `Client.EnqueueContext` copies composed
    /// options into `base.TaskMessage` before enqueueing:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L295-L324>.
    fn apply_options(&mut self, options: &[TaskOption]) {
        for opt in options {
            match opt {
                TaskOption::MaxRetry(retry) => {
                    self.retry = (*retry).try_into().unwrap_or(i32::MAX);
                }
                TaskOption::Queue(queue) => {
                    self.queue.clone_from(queue);
                }
                TaskOption::TaskId(id) => {
                    self.id.clone_from(id);
                }
                TaskOption::Timeout(timeout) => {
                    self.timeout = duration_seconds(*timeout);
                }
                TaskOption::Deadline(deadline) => {
                    self.deadline = unix_seconds(*deadline);
                }
                TaskOption::Retention(retention) => {
                    self.retention = duration_seconds(*retention);
                }
                TaskOption::Group(group) => {
                    self.group_key.clone_from(group);
                }
                TaskOption::Unique(_) => {
                    // TODO: Derive `unique_key` once Redis key helpers and
                    // enqueue uniqueness behavior are modeled. Upstream builds
                    // it from queue name, task type, and payload.
                }
                TaskOption::ProcessAt(_) | TaskOption::ProcessIn(_) => {
                    // TODO: Apply scheduling options when the enqueue path can
                    // choose between pending, scheduled, and aggregating states.
                }
            }
        }
    }
}

fn duration_seconds(duration: Duration) -> i64 {
    duration.as_secs().try_into().unwrap_or(i64::MAX)
}

fn unix_seconds(time: SystemTime) -> i64 {
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
    use std::time::{Duration, UNIX_EPOCH};

    use crate::TaskOption;

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

    #[test]
    fn packs_task_options_with_message_fields() {
        let deadline = UNIX_EPOCH + Duration::from_secs(1_234_567_890);
        let task = Task::new_with_options(
            "email:welcome",
            b"{}".to_vec(),
            [
                TaskOption::task_id("custom-id"),
                TaskOption::queue("critical"),
                TaskOption::max_retry(3),
                TaskOption::timeout(Duration::from_secs(30)),
                TaskOption::deadline(deadline),
                TaskOption::retention(Duration::from_secs(300)),
                TaskOption::group("tenant-a"),
            ],
        );

        let msg = TaskMessage::from_task(&task);

        assert_eq!(msg.id, "custom-id");
        assert_eq!(msg.queue, "critical");
        assert_eq!(msg.retry, 3);
        assert_eq!(msg.timeout, 30);
        assert_eq!(msg.deadline, 1_234_567_890);
        assert_eq!(msg.retention, 300);
        assert_eq!(msg.group_key, "tenant-a");
    }

    #[test]
    fn later_task_options_override_earlier_values() {
        let task = Task::new_with_options(
            "email:welcome",
            Vec::new(),
            [
                TaskOption::queue("low"),
                TaskOption::queue("critical"),
                TaskOption::max_retry(1),
                TaskOption::max_retry(5),
                TaskOption::timeout(Duration::from_secs(10)),
                TaskOption::timeout(Duration::from_secs(20)),
            ],
        );

        let msg = TaskMessage::from_task(&task);

        assert_eq!(msg.queue, "critical");
        assert_eq!(msg.retry, 5);
        assert_eq!(msg.timeout, 20);
    }
}
