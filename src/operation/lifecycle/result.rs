use bytes::Bytes;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ResultError {
    #[error("task has no result writer")]
    NoResultWriter,
    #[error("failed to write task result: {0}")]
    WriteFailed(String),
    #[error("{0}")]
    Other(String),
}

impl ResultError {
    pub fn write_failed(message: impl Into<String>) -> Self {
        Self::WriteFailed(message.into())
    }

    pub fn other(message: impl Into<String>) -> Self {
        Self::Other(message.into())
    }

    pub fn is_writer_closed(&self) -> bool {
        matches!(self, Self::WriteFailed(message) if message == "result writer is closed")
    }

    pub fn is_context_cancelled(&self) -> bool {
        matches!(self, Self::WriteFailed(message) if message == "context canceled")
    }

    pub fn is_context_deadline_exceeded(&self) -> bool {
        matches!(self, Self::WriteFailed(message) if message == "context deadline exceeded")
    }
}

pub(crate) struct ResultWrite {
    pub(crate) data: Bytes,
    pub(crate) ack: Option<oneshot::Sender<Result<usize, ResultError>>>,
    pub(crate) cancellation: Option<CancellationToken>,
    pub(crate) parent_cancellation: Option<CancellationToken>,
    pub(crate) deadline: Option<tokio::time::Instant>,
}

/// Worker-side result writer associated with a handler task.
///
/// Reference: Asynq v0.26.0 `Task.ResultWriter`, `ResultWriter.Write`, and
/// `ResultWriter.TaskID`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L42-L43>
/// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L555-L575>.
///
#[derive(Debug, Clone)]
pub struct ResultWriter {
    task_id: String,
    sender: mpsc::UnboundedSender<ResultWrite>,
    cancellation: Option<CancellationToken>,
    parent_cancellation: Option<CancellationToken>,
    deadline: Option<tokio::time::Instant>,
}

impl ResultWriter {
    pub(crate) fn channel_with_context(
        task_id: impl Into<String>,
        cancellation: CancellationToken,
        parent_cancellation: Option<CancellationToken>,
        deadline: Option<tokio::time::Instant>,
    ) -> (Self, mpsc::UnboundedReceiver<ResultWrite>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (
            Self {
                task_id: task_id.into(),
                sender,
                cancellation: Some(cancellation),
                parent_cancellation,
                deadline,
            },
            receiver,
        )
    }

    /// Returns the ID of the task this result writer is associated with.
    ///
    /// Reference: Asynq v0.26.0 `ResultWriter.TaskID`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L574-L577>.
    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    pub fn write(&self, data: impl Into<Bytes>) -> Result<usize, ResultError> {
        self.check_context()?;
        let data = data.into();
        let len = data.len();
        self.sender
            .send(ResultWrite {
                data,
                ack: None,
                cancellation: self.cancellation.clone(),
                parent_cancellation: self.parent_cancellation.clone(),
                deadline: self.deadline,
            })
            .map_err(|_| ResultError::WriteFailed("result writer is closed".to_owned()))?;
        Ok(len)
    }

    /// Writes result data and waits until the worker persists it through the
    /// broker.
    ///
    /// Reference: Asynq v0.26.0 `ResultWriter.Write` calls Redis
    /// `WriteResult` directly and returns its acknowledgement:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L560-L572>.
    pub async fn write_async(&self, data: impl Into<Bytes>) -> Result<usize, ResultError> {
        self.check_context()?;
        let data = data.into();
        let (ack, wait_for_ack) = oneshot::channel();
        self.sender
            .send(ResultWrite {
                data,
                ack: Some(ack),
                cancellation: self.cancellation.clone(),
                parent_cancellation: self.parent_cancellation.clone(),
                deadline: self.deadline,
            })
            .map_err(|_| ResultError::WriteFailed("result writer is closed".to_owned()))?;
        wait_for_ack.await.map_err(|_| {
            ResultError::WriteFailed("result writer acknowledgement channel is closed".to_owned())
        })?
    }

    fn check_context(&self) -> Result<(), ResultError> {
        // Reference: Asynq v0.26.0 `ResultWriter.Write` checks the handler
        // context before writing result data:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L560-L572>.
        result_context_error(&self.cancellation, &self.parent_cancellation, self.deadline)
            .map_or(Ok(()), Err)
    }
}

impl ResultWrite {
    pub(crate) fn context_error(&self) -> Option<ResultError> {
        // Reference: Asynq v0.26.0 `ResultWriter.Write` passes the handler
        // context into Redis `WriteResult`, so cancellation/deadline changes
        // that happen after the write request is created but before broker IO
        // still prevent result persistence:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L560-L572>.
        result_context_error(&self.cancellation, &self.parent_cancellation, self.deadline)
    }
}

fn result_context_error(
    cancellation: &Option<CancellationToken>,
    parent_cancellation: &Option<CancellationToken>,
    deadline: Option<tokio::time::Instant>,
) -> Option<ResultError> {
    if cancellation
        .as_ref()
        .is_some_and(CancellationToken::is_cancelled)
        || parent_cancellation
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
    {
        return Some(ResultError::WriteFailed("context canceled".to_owned()));
    }
    if deadline.is_some_and(|deadline| deadline <= tokio::time::Instant::now()) {
        return Some(ResultError::WriteFailed(
            "context deadline exceeded".to_owned(),
        ));
    }
    None
}

impl std::io::Write for ResultWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        ResultWriter::write(self, Bytes::copy_from_slice(buf))
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::BrokenPipe, error))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
