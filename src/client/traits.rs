use crate::{BrokerError, EnqueuePlan};

/// Minimal broker interface for the client enqueue path.
///
/// Reference: Asynq v0.26.0 internal `base.Broker` enqueue-related methods:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L371-L419>.
pub trait Broker {
    fn ping(&mut self) -> Result<(), BrokerError>;

    fn enqueue(&mut self, plan: &EnqueuePlan) -> Result<(), BrokerError>;
}

/// Async broker interface for Redis-backed client enqueue operations.
///
/// Reference: Asynq v0.26.0 `Client.EnqueueContext` passes the provided
/// `context.Context` to the Redis-backed broker operation:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L314-L331>.
#[async_trait::async_trait]
pub trait AsyncBroker {
    async fn ping(&mut self) -> Result<(), BrokerError>;

    async fn enqueue(&mut self, plan: &EnqueuePlan) -> Result<(), BrokerError>;
}

/// Minimal broker interface for closing client-owned backing resources.
///
/// Reference: Asynq v0.26.0 `Client.Close` delegates to the underlying
/// broker unless the Redis connection is shared:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L207-L215>.
pub trait CloseBroker {
    fn close(&mut self) -> Result<(), BrokerError>;
}
