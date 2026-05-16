use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RetryError {
    #[error("task not found")]
    NotFound,
    #[error("{0}")]
    Other(String),
}
