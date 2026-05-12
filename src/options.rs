use std::time::{Duration, SystemTime};

/// Task processing option.
///
/// Reference: Asynq v0.26.0 public `Option` constructors:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L47-L163>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskOption {
    MaxRetry(u32),
    Queue(String),
    TaskId(String),
    Timeout(Duration),
    Deadline(SystemTime),
    Unique(Duration),
    ProcessAt(SystemTime),
    ProcessIn(Duration),
    Retention(Duration),
    Group(String),
}

impl TaskOption {
    pub fn max_retry(n: i32) -> Self {
        Self::MaxRetry(n.max(0) as u32)
    }

    pub fn queue(name: impl Into<String>) -> Self {
        Self::Queue(name.into())
    }

    pub fn task_id(id: impl Into<String>) -> Self {
        Self::TaskId(id.into())
    }

    pub fn timeout(duration: Duration) -> Self {
        Self::Timeout(duration)
    }

    pub fn deadline(time: SystemTime) -> Self {
        Self::Deadline(time)
    }

    pub fn unique(ttl: Duration) -> Self {
        Self::Unique(ttl)
    }

    pub fn process_at(time: SystemTime) -> Self {
        Self::ProcessAt(time)
    }

    pub fn process_in(duration: Duration) -> Self {
        Self::ProcessIn(duration)
    }

    pub fn retention(duration: Duration) -> Self {
        Self::Retention(duration)
    }

    pub fn group(name: impl Into<String>) -> Self {
        Self::Group(name.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_retry_clamps_negative_values() {
        assert_eq!(TaskOption::max_retry(-1), TaskOption::MaxRetry(0));
    }
}
