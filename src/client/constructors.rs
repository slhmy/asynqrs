use super::{Client, SystemClock, UuidTaskIdGenerator};
use crate::broker::redis::{
    MakeRedisClientError, RedisRuntimeBroker, RedisRuntimeClient, redis_broker_from_runtime_client,
};

/// Concrete client type backed by a Redis multiplexed connection.
///
/// Reference: Asynq v0.26.0 public `Client` created by
/// `NewClientFromRedisClient`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L48-L56>.
pub type RedisBackedClient = Client<RedisRuntimeBroker, UuidTaskIdGenerator, SystemClock>;

impl RedisBackedClient {
    pub async fn from_redis_client(
        redis_client: impl Into<RedisRuntimeClient>,
    ) -> Result<Self, MakeRedisClientError> {
        Self::from_redis_runtime_client(redis_client.into()).await
    }

    pub async fn from_redis_runtime_client(
        redis_client: RedisRuntimeClient,
    ) -> Result<Self, MakeRedisClientError> {
        new_client_from_redis_runtime_client(redis_client, true).await
    }

    pub async fn from_direct_redis_client(
        redis_client: redis::Client,
    ) -> Result<Self, MakeRedisClientError> {
        Self::from_redis_runtime_client(RedisRuntimeClient::direct(redis_client)).await
    }
}

async fn new_client_from_redis_runtime_client(
    redis_client: RedisRuntimeClient,
    shared_connection: bool,
) -> Result<RedisBackedClient, MakeRedisClientError> {
    let broker = redis_broker_from_runtime_client(redis_client).await?;
    if shared_connection {
        Ok(Client::with_shared_connection(
            broker,
            UuidTaskIdGenerator,
            SystemClock,
        ))
    } else {
        Ok(Client::with_parts(broker, UuidTaskIdGenerator, SystemClock))
    }
}
