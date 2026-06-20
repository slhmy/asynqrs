use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ArchiveError {
    #[error("task not found")]
    NotFound,
    #[error("{0}")]
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CancelError {
    #[error("{0}")]
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CleanupError {
    #[error("{0}")]
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CompleteError {
    #[error("task not found")]
    NotFound,
    #[error("{0}")]
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ForwardError {
    #[error("{0}")]
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LeaseError {
    #[error("{0}")]
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RecoverError {
    #[error("{0}")]
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RequeueError {
    #[error("task not found")]
    NotFound,
    #[error("{0}")]
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RetryError {
    #[error("task not found")]
    NotFound,
    #[error("{0}")]
    Other(String),
}
