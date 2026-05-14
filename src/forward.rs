/// Minimal broker interface for moving due scheduled/retry tasks back to
/// processable queues.
///
/// Reference: Asynq v0.26.0 `RDB.ForwardIfReady` and `forwardCmd`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L861-L900>.
///
/// TODO: Add the full server-side forwarder loop once worker `Server` is
/// modeled. This interface moves one upstream batch of up to 100 due tasks.
pub trait ForwardBroker {
    fn forward_scheduled(&mut self, queue: &str) -> Result<usize, ForwardError>;

    fn forward_retry(&mut self, queue: &str) -> Result<usize, ForwardError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForwardError {
    Other(String),
}

impl std::fmt::Display for ForwardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Other(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for ForwardError {}
