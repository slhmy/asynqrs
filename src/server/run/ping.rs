use crate::server::error::ServerError;
use crate::server::worker::ServerConnection;
use crate::server::{Server, Sleeper};

impl<P, S> Server<P, S>
where
    P: ServerConnection + Send,
    S: Sleeper + Send,
{
    /// Checks whether the backing broker is reachable.
    ///
    /// Reference: Asynq v0.26.0 `Server.Ping` delegates to `Broker.Ping` and
    /// returns successfully after shutdown:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L776-L786>.
    pub async fn ping(&mut self) -> Result<(), ServerError> {
        if self.state().is_closed() {
            return Ok(());
        }
        self.runtime.ping().await.map_err(ServerError::Ping)
    }
}
