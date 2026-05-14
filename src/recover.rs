use std::time::SystemTime;

/// Summary of lease-expired active tasks recovered in one batch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoverResult {
    retried: usize,
    archived: usize,
}

/// Minimal broker interface for recovering active tasks whose leases expired.
///
/// Reference: Asynq v0.26.0 recoverer lists lease-expired tasks and sends them
/// through retry/archive lifecycle paths:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/recoverer.go>.
///
/// TODO: Add the full server-side recoverer loop once worker `Server` is
/// modeled, including the 30-second clock-skew cutoff and default retry delay
/// calculation.
pub trait RecoverBroker {
    fn recover_expired_leases(
        &mut self,
        queue: &str,
        retry_at: SystemTime,
        error_message: &str,
    ) -> Result<RecoverResult, RecoverError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoverError {
    Other(String),
}

impl RecoverResult {
    pub fn new(retried: usize, archived: usize) -> Self {
        Self { retried, archived }
    }

    pub fn retried(&self) -> usize {
        self.retried
    }

    pub fn archived(&self) -> usize {
        self.archived
    }

    pub fn total(&self) -> usize {
        self.retried + self.archived
    }
}

impl std::fmt::Display for RecoverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Other(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for RecoverError {}
