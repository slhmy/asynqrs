use std::sync::Arc;

use super::super::{CancellationListener, Server};

impl<P, S> Server<P, S> {
    pub fn with_shared_connection(mut self) -> Self {
        // Reference: Asynq v0.26.0 `NewServerFromRedisClient` uses a
        // caller-owned Redis client, so server shutdown must not close the
        // underlying connection pool:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L431-L620>.
        self.shared_connection = true;
        self
    }

    pub fn with_cancellation_listener<L>(mut self, listener: L) -> Self
    where
        L: CancellationListener + 'static,
    {
        self.cancellation_listener = Some(Arc::new(listener));
        self
    }
}
