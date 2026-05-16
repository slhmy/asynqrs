use std::time::SystemTime;

use thiserror::Error;

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
