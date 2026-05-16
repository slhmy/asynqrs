use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ArchiveError {
    #[error("task not found")]
    NotFound,
    #[error("{0}")]
    Other(String),
}
