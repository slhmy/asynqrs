use std::time::Duration;

use crate::MetadataError;
use crate::broker::redis::RedisExecutor;
use crate::client::Clock;
use crate::server::MetadataBroker;

use super::super::RedisBroker;

#[async_trait::async_trait]
impl<E, C> MetadataBroker for RedisBroker<E, C>
where
    E: RedisExecutor + Send,
    C: Clock + Send + Sync,
{
    async fn write_server_state(
        &mut self,
        hostname: &str,
        pid: i32,
        server_id: &str,
        server_info: Vec<u8>,
        workers: Vec<Vec<u8>>,
        ttl: Duration,
    ) -> Result<(), MetadataError> {
        RedisBroker::write_server_state(self, hostname, pid, server_id, server_info, workers, ttl)
            .await
    }

    async fn clear_server_state(
        &mut self,
        hostname: &str,
        pid: i32,
        server_id: &str,
    ) -> Result<(), MetadataError> {
        RedisBroker::clear_server_state(self, hostname, pid, server_id).await
    }
}
