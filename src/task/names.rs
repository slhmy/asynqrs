use std::fmt;

use thiserror::Error;

use super::{QueueNameError, validate_queue_name};

/// Valid queue name carried as a semantic value.
///
/// Reference: Asynq v0.26.0 queue names are validated with
/// `base.ValidateQueueName` before queue operations:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L96-L103>.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct QueueName(String);

impl QueueName {
    pub fn new(name: impl Into<String>) -> Result<Self, QueueNameError> {
        let name = name.into();
        validate_queue_name(&name)?;
        Ok(Self(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl TryFrom<String> for QueueName {
    type Error = QueueNameError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for QueueName {
    type Error = QueueNameError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<QueueName> for String {
    fn from(value: QueueName) -> Self {
        value.into_string()
    }
}

impl AsRef<str> for QueueName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for QueueName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("{kind} must contain one or more non-whitespace characters")]
pub struct NonBlankNameError {
    kind: &'static str,
}

impl NonBlankNameError {
    pub fn kind(&self) -> &'static str {
        self.kind
    }
}

/// Valid task ID carried as a semantic value.
///
/// Reference: Asynq v0.26.0 rejects blank task IDs while composing enqueue
/// options:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L299-L312>.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TaskId(String);

impl TaskId {
    pub fn new(id: impl Into<String>) -> Result<Self, NonBlankNameError> {
        non_blank(id, "task id").map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl TryFrom<String> for TaskId {
    type Error = NonBlankNameError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for TaskId {
    type Error = NonBlankNameError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<TaskId> for String {
    fn from(value: TaskId) -> Self {
        value.into_string()
    }
}

impl AsRef<str> for TaskId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Valid group name carried as a semantic value.
///
/// Reference: Asynq v0.26.0 rejects blank group keys while composing enqueue
/// options:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L299-L312>.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GroupName(String);

impl GroupName {
    pub fn new(name: impl Into<String>) -> Result<Self, NonBlankNameError> {
        non_blank(name, "group name").map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl TryFrom<String> for GroupName {
    type Error = NonBlankNameError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for GroupName {
    type Error = NonBlankNameError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<GroupName> for String {
    fn from(value: GroupName) -> Self {
        value.into_string()
    }
}

impl AsRef<str> for GroupName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for GroupName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Task type carried as a semantic value.
///
/// Reference: Asynq v0.26.0 stores task type as an opaque string on public
/// `Task` values:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L22-L40>.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TaskType(String);

impl TaskType {
    pub fn new(type_name: impl Into<String>) -> Self {
        Self(type_name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl From<String> for TaskType {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for TaskType {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<TaskType> for String {
    fn from(value: TaskType) -> Self {
        value.into_string()
    }
}

impl AsRef<str> for TaskType {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for TaskType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

fn non_blank(value: impl Into<String>, kind: &'static str) -> Result<String, NonBlankNameError> {
    let value = value.into();
    if value.trim().is_empty() {
        return Err(NonBlankNameError { kind });
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_name_validates_using_upstream_rule() {
        assert_eq!(QueueName::new("").unwrap_err(), QueueNameError);
        assert_eq!(QueueName::new("critical").unwrap().as_str(), "critical");
    }

    #[test]
    fn task_id_and_group_name_reject_blank_values() {
        assert_eq!(TaskId::new(" ").unwrap_err().kind(), "task id");
        assert_eq!(GroupName::new("\t").unwrap_err().kind(), "group name");
        assert_eq!(TaskId::new("task-1").unwrap().to_string(), "task-1");
        assert_eq!(GroupName::new("tenant-a").unwrap().to_string(), "tenant-a");
    }

    #[test]
    fn task_type_is_opaque_like_upstream_task_type() {
        let task_type = TaskType::new("email:welcome");
        assert_eq!(task_type.as_str(), "email:welcome");

        let owned: String = task_type.into();
        assert_eq!(owned, "email:welcome");
    }
}
