use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ForwardError {
    #[error("{0}")]
    Other(String),
}
