use tokio_util::sync::CancellationToken;

/// Cancellation scope for a client enqueue operation.
///
/// Reference: Asynq v0.26.0 public `Client.EnqueueContext` accepts a
/// `context.Context` as the first argument for the enqueue operation:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L266-L331>.
#[derive(Debug, Clone, Default)]
pub struct ClientEnqueueScope {
    cancellation: CancellationToken,
}

impl ClientEnqueueScope {
    pub fn background() -> Self {
        Self::default()
    }

    pub fn cancelled() -> Self {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        Self::from_cancellation_token(cancellation)
    }

    pub fn from_cancellation_token(cancellation: CancellationToken) -> Self {
        Self { cancellation }
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    pub(in crate::client) async fn wait_cancelled(&self) {
        self.cancellation.cancelled().await;
    }
}
