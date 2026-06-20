use super::{SystemClock, UuidTaskIdGenerator};
use crate::TaskInfo;

/// Client API for enqueueing tasks through a broker.
///
/// Reference: Asynq v0.26.0 `Client.Enqueue` and `Client.EnqueueContext`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L266-L331>.
#[derive(Debug, Clone)]
pub struct Client<B, G = UuidTaskIdGenerator, C = SystemClock> {
    pub(super) broker: B,
    pub(super) shared_connection: bool,
    pub(super) task_id_generator: G,
    pub(super) clock: C,
}

impl<B> Client<B, UuidTaskIdGenerator, SystemClock> {
    pub fn new(broker: B) -> Self {
        Self::with_parts(broker, UuidTaskIdGenerator, SystemClock)
    }
}

impl<B, G> Client<B, G, SystemClock> {
    pub fn with_task_id_generator(broker: B, task_id_generator: G) -> Self {
        Self::with_parts(broker, task_id_generator, SystemClock)
    }
}

impl<B, G, C> Client<B, G, C> {
    pub fn with_parts(broker: B, task_id_generator: G, clock: C) -> Self {
        Self {
            broker,
            shared_connection: false,
            task_id_generator,
            clock,
        }
    }

    /// Creates a client backed by caller-owned broker resources.
    ///
    /// Reference: Asynq v0.26.0 `NewClientFromRedisClient` marks its Redis
    /// connection as shared so `Client.Close` refuses to close it:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L44-L48>.
    pub fn with_shared_connection(broker: B, task_id_generator: G, clock: C) -> Self {
        Self {
            broker,
            shared_connection: true,
            task_id_generator,
            clock,
        }
    }
}

/// Result returned after a successful enqueue.
///
/// Reference: Asynq v0.26.0 `Client.Enqueue` returns a `TaskInfo` populated
/// from the enqueued task message:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L316-L331>.
pub type EnqueueResult = TaskInfo;
