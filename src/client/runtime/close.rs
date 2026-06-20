use super::{Client, ClientError};
use crate::client::CloseBroker;

impl<B, G, C> Client<B, G, C>
where
    B: CloseBroker,
{
    /// Closes the client-owned backing broker resources.
    ///
    /// Reference: Asynq v0.26.0 `Client.Close` delegates to `Broker.Close`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L319-L324>.
    pub fn close(&mut self) -> Result<(), ClientError> {
        if self.shared_connection {
            return Err(ClientError::SharedConnection);
        }
        self.broker.close().map_err(ClientError::Close)
    }
}
