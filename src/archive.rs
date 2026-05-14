use std::time::SystemTime;

use crate::TaskMessage;

/// Minimal broker interface for archiving a failed active task.
///
/// Reference: Asynq v0.26.0 archive-related task state handling in
/// `internal/rdb`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.
///
/// TODO: Add lease-expiration recovery and inspector-style archive management
/// once the full failure and admin lifecycle is modeled.
pub trait ArchiveBroker {
    fn archive(
        &mut self,
        message: &TaskMessage,
        archived_at: SystemTime,
        error_message: &str,
        is_failure: bool,
    ) -> Result<(), ArchiveError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArchiveError {
    NotFound,
    Other(String),
}

impl std::fmt::Display for ArchiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => f.write_str("task not found"),
            Self::Other(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for ArchiveError {}
