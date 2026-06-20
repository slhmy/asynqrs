use super::{Server, ServerError};

/// Runtime lifecycle state for a `Server`.
///
/// Reference: Asynq v0.26.0 `serverStateValue` tracks `new`, `active`,
/// `stopped`, and `closed` before allowing `Start`, `Stop`, `Shutdown`, or
/// `Ping` behavior:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L45-L77>.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ServerState {
    New,
    Active,
    Closed,
}

impl<P, S> Server<P, S> {
    pub(in crate::server) fn begin_start(&mut self) -> Result<(), ServerError> {
        match self.state {
            ServerState::New => {
                self.state = ServerState::Active;
                Ok(())
            }
            ServerState::Active => Err(ServerError::AlreadyRunning),
            ServerState::Closed => Err(ServerError::Closed),
        }
    }

    pub(in crate::server) fn mark_closed(&mut self) {
        self.state = ServerState::Closed;
    }

    pub(crate) fn state(&self) -> ServerState {
        self.state
    }
}

impl ServerState {
    pub(crate) fn is_closed(self) -> bool {
        matches!(self, Self::Closed)
    }
}
