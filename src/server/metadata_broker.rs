use std::time::Duration;

use async_trait::async_trait;

use crate::MetadataError;

/// Async broker interface for server heartbeat metadata.
///
/// Reference: Asynq v0.26.0 `RDB.WriteServerState` and
/// `RDB.ClearServerState`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.
#[async_trait]
pub(crate) trait MetadataBroker {
    async fn write_server_state(
        &mut self,
        hostname: &str,
        pid: i32,
        server_id: &str,
        server_info: Vec<u8>,
        workers: Vec<Vec<u8>>,
        ttl: Duration,
    ) -> Result<(), MetadataError>;

    async fn clear_server_state(
        &mut self,
        hostname: &str,
        pid: i32,
        server_id: &str,
    ) -> Result<(), MetadataError>;
}

/// Async broker interface for checking Redis connectivity.
///
/// Reference: Asynq v0.26.0 `Server.Ping` delegates to `Broker.Ping`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L776-L786>.
#[async_trait]
pub(crate) trait PingBroker {
    async fn ping(&mut self) -> Result<(), String>;
}
