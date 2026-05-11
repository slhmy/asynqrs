use std::fmt;
use std::str::FromStr;

/// State of a task in the queue lifecycle.
///
/// Reference: Asynq v0.26.0 public `TaskState` constants and `String` mapping:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L207-L250>.
///
/// Keep the variant order and string representation aligned with upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum TaskState {
    /// The task is currently being processed by a worker.
    Active = 1,
    /// The task is ready to be processed by a worker.
    Pending,
    /// The task is scheduled to be processed in the future.
    Scheduled,
    /// The task failed before and is scheduled to be retried.
    Retry,
    /// The task is archived for inspection.
    Archived,
    /// The task was processed successfully and is retained until its TTL expires.
    Completed,
    /// The task is waiting in a group before aggregation.
    Aggregating,
}

impl TaskState {
    pub const ALL: [Self; 7] = [
        Self::Active,
        Self::Pending,
        Self::Scheduled,
        Self::Retry,
        Self::Archived,
        Self::Completed,
        Self::Aggregating,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Pending => "pending",
            Self::Scheduled => "scheduled",
            Self::Retry => "retry",
            Self::Archived => "archived",
            Self::Completed => "completed",
            Self::Aggregating => "aggregating",
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Archived | Self::Completed)
    }
}

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TaskState {
    type Err = ParseTaskStateError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "active" => Ok(Self::Active),
            "pending" => Ok(Self::Pending),
            "scheduled" => Ok(Self::Scheduled),
            "retry" => Ok(Self::Retry),
            "archived" => Ok(Self::Archived),
            "completed" => Ok(Self::Completed),
            "aggregating" => Ok(Self::Aggregating),
            _ => Err(ParseTaskStateError {
                value: s.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseTaskStateError {
    value: String,
}

impl ParseTaskStateError {
    pub fn value(&self) -> &str {
        &self.value
    }
}

impl fmt::Display for ParseTaskStateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown task state: {}", self.value)
    }
}

impl std::error::Error for ParseTaskStateError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_state_round_trips_asynq_strings() {
        for state in TaskState::ALL {
            assert_eq!(state.as_str().parse::<TaskState>(), Ok(state));
            assert_eq!(state.to_string(), state.as_str());
        }
    }

    #[test]
    fn identifies_terminal_states() {
        assert!(TaskState::Archived.is_terminal());
        assert!(TaskState::Completed.is_terminal());

        assert!(!TaskState::Active.is_terminal());
        assert!(!TaskState::Pending.is_terminal());
        assert!(!TaskState::Scheduled.is_terminal());
        assert!(!TaskState::Retry.is_terminal());
        assert!(!TaskState::Aggregating.is_terminal());
    }

    #[test]
    fn parse_error_keeps_original_value() {
        let err = "unknown".parse::<TaskState>().unwrap_err();

        assert_eq!(err.value(), "unknown");
        assert_eq!(err.to_string(), "unknown task state: unknown");
    }
}
