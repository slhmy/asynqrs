use std::time::SystemTime;

use thiserror::Error;

/// Minimal broker interface for extending the lease of an active task.
///
/// Reference: Asynq v0.26.0 `RDB.ExtendLease` updates active-task lease scores
/// with Redis `ZADD XX`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.
///
/// TODO: Add the full worker lease extender loop once worker `Server` and
/// handler execution are modeled.
pub trait LeaseBroker {
    fn extend_lease(&mut self, queue: &str, task_id: &str) -> Result<LeaseExtension, LeaseError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseExtension {
    expires_at: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LeaseError {
    #[error("{0}")]
    Other(String),
}

impl LeaseExtension {
    pub fn new(expires_at: SystemTime) -> Self {
        Self { expires_at }
    }

    pub fn expires_at(&self) -> SystemTime {
        self.expires_at
    }
}
