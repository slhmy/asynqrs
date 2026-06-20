use std::{fmt, sync::Arc, time::Duration};

use futures_util::FutureExt as _;
use thiserror::Error;

/// Error returned when building runtime Redis connections from redis-rs clients.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MakeRedisClientError {
    #[error("asynq: unsupported redis connection option: {0}")]
    UnsupportedConnectionOption(String),
    #[error("asynq: could not create redis client: {0}")]
    Redis(String),
}

/// Default dial timeout used by Asynq Redis option zero values.
///
/// Reference: Asynq v0.26.0 public Redis option docs state zero
/// `DialTimeout` uses go-redis' five-second default:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L285-L287>.
const DEFAULT_ASYNQ_REDIS_DIAL_TIMEOUT: Duration = Duration::from_secs(5);
/// Default read timeout used by Asynq Redis option zero values.
///
/// Reference: Asynq v0.26.0 public Redis option docs state zero
/// `ReadTimeout` uses go-redis' three-second default:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L289-L295>.
const DEFAULT_ASYNQ_REDIS_READ_TIMEOUT: Duration = Duration::from_secs(3);

/// Redis client handle used by Redis-backed runtimes.
///
/// Reference: Asynq v0.26.0 constructors and internal runtime code use
/// `redis.UniversalClient` so direct, failover, and cluster clients can share
/// the same call sites:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L257-L265>.
pub enum RedisRuntimeClient {
    Direct(RedisDirectRuntimeClient),
    Failover(RedisFailoverRuntimeClient),
    Cluster(RedisClusterRuntimeClient),
}

/// Redis runtime timeout overrides derived from Asynq Redis option fields.
///
/// Zero Asynq timeout option values are normalized to Asynq/go-redis defaults
/// instead of redis-rs defaults so Redis-backed runtimes behave like upstream
/// constructors.
///
/// Reference: Asynq v0.26.0 public Redis client option timeout fields:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L267-L431>.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RedisRuntimeTimeouts {
    connection_timeout: Option<Duration>,
    response_timeout: Option<Duration>,
}

impl RedisRuntimeTimeouts {
    pub fn new(connection_timeout: Option<Duration>, response_timeout: Option<Duration>) -> Self {
        Self {
            connection_timeout,
            response_timeout,
        }
    }

    pub fn from_asynq_timeouts(dial_timeout: Duration, read_timeout: Duration) -> Self {
        Self {
            connection_timeout: Some(asynq_timeout_or_default(
                dial_timeout,
                DEFAULT_ASYNQ_REDIS_DIAL_TIMEOUT,
            )),
            response_timeout: Some(asynq_timeout_or_default(
                read_timeout,
                DEFAULT_ASYNQ_REDIS_READ_TIMEOUT,
            )),
        }
    }

    pub fn connection_timeout(&self) -> Option<Duration> {
        self.connection_timeout
    }

    pub fn response_timeout(&self) -> Option<Duration> {
        self.response_timeout
    }

    pub(crate) fn async_connection_config(&self) -> redis::AsyncConnectionConfig {
        let mut config = redis::AsyncConnectionConfig::new();
        if let Some(timeout) = self.connection_timeout {
            config = config.set_connection_timeout(Some(timeout));
        }
        if let Some(timeout) = self.response_timeout {
            config = config.set_response_timeout(Some(timeout));
        }
        config
    }

    pub(crate) fn cluster_config(&self) -> redis::cluster::ClusterConfig {
        let mut config = redis::cluster::ClusterConfig::new();
        if let Some(timeout) = self.connection_timeout {
            config = config.set_connection_timeout(timeout);
        }
        if let Some(timeout) = self.response_timeout {
            config = config.set_response_timeout(timeout);
        }
        config
    }
}

#[derive(Debug, Clone)]
pub struct RedisDirectRuntimeClient {
    client: redis::Client,
    timeouts: RedisRuntimeTimeouts,
}

impl RedisDirectRuntimeClient {
    pub fn new(client: redis::Client) -> Self {
        Self::with_timeouts(client, RedisRuntimeTimeouts::default())
    }

    pub fn with_timeouts(client: redis::Client, timeouts: RedisRuntimeTimeouts) -> Self {
        Self { client, timeouts }
    }

    pub fn client(&self) -> &redis::Client {
        &self.client
    }

    pub fn into_client(self) -> redis::Client {
        self.client
    }

    pub fn timeouts(&self) -> RedisRuntimeTimeouts {
        self.timeouts
    }

    fn async_connection_config(&self) -> redis::AsyncConnectionConfig {
        self.timeouts.async_connection_config()
    }
}

#[derive(Clone)]
pub struct RedisFailoverRuntimeClient {
    client: Arc<tokio::sync::Mutex<redis::sentinel::SentinelClient>>,
    timeouts: RedisRuntimeTimeouts,
}

impl RedisFailoverRuntimeClient {
    pub fn new(client: redis::sentinel::SentinelClient) -> Self {
        Self::with_timeouts(client, RedisRuntimeTimeouts::default())
    }

    pub fn with_timeouts(
        client: redis::sentinel::SentinelClient,
        timeouts: RedisRuntimeTimeouts,
    ) -> Self {
        Self {
            client: Arc::new(tokio::sync::Mutex::new(client)),
            timeouts,
        }
    }

    pub fn client(&self) -> &Arc<tokio::sync::Mutex<redis::sentinel::SentinelClient>> {
        &self.client
    }

    pub fn timeouts(&self) -> RedisRuntimeTimeouts {
        self.timeouts
    }

    fn async_connection_config(&self) -> redis::AsyncConnectionConfig {
        self.timeouts.async_connection_config()
    }
}

impl fmt::Debug for RedisFailoverRuntimeClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisFailoverRuntimeClient")
            .field("client", &"SentinelClient")
            .field("timeouts", &self.timeouts)
            .finish()
    }
}

#[derive(Clone)]
pub struct RedisClusterRuntimeClient {
    client: Arc<redis::cluster::ClusterClient>,
    timeouts: RedisRuntimeTimeouts,
    max_redirects: Option<u32>,
}

impl fmt::Debug for RedisClusterRuntimeClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisClusterRuntimeClient")
            .field("client", &"ClusterClient")
            .field("timeouts", &self.timeouts)
            .field("max_redirects", &self.max_redirects)
            .finish()
    }
}

impl RedisClusterRuntimeClient {
    pub fn new(client: redis::cluster::ClusterClient) -> Self {
        Self::with_timeouts(client, RedisRuntimeTimeouts::default())
    }

    pub fn with_timeouts(
        client: redis::cluster::ClusterClient,
        timeouts: RedisRuntimeTimeouts,
    ) -> Self {
        Self::with_timeouts_and_max_redirects(client, timeouts, None)
    }

    pub fn with_timeouts_and_max_redirects(
        client: redis::cluster::ClusterClient,
        timeouts: RedisRuntimeTimeouts,
        max_redirects: Option<u32>,
    ) -> Self {
        Self {
            client: Arc::new(client),
            timeouts,
            max_redirects,
        }
    }

    pub fn client(&self) -> &Arc<redis::cluster::ClusterClient> {
        &self.client
    }

    pub fn timeouts(&self) -> RedisRuntimeTimeouts {
        self.timeouts
    }

    pub fn max_redirects(&self) -> Option<u32> {
        self.max_redirects
    }

    fn cluster_config(&self) -> redis::cluster::ClusterConfig {
        self.timeouts.cluster_config()
    }
}

impl fmt::Debug for RedisRuntimeClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Direct(client) => f.debug_tuple("Direct").field(client).finish(),
            Self::Failover(client) => f.debug_tuple("Failover").field(client).finish(),
            Self::Cluster(client) => f.debug_tuple("Cluster").field(client).finish(),
        }
    }
}

impl Clone for RedisRuntimeClient {
    fn clone(&self) -> Self {
        match self {
            Self::Direct(client) => Self::Direct(client.clone()),
            Self::Failover(client) => Self::Failover(client.clone()),
            Self::Cluster(client) => Self::Cluster(client.clone()),
        }
    }
}

impl From<redis::Client> for RedisRuntimeClient {
    fn from(client: redis::Client) -> Self {
        Self::direct(client)
    }
}

impl From<redis::sentinel::SentinelClient> for RedisRuntimeClient {
    fn from(client: redis::sentinel::SentinelClient) -> Self {
        Self::failover(client)
    }
}

impl From<redis::cluster::ClusterClient> for RedisRuntimeClient {
    fn from(client: redis::cluster::ClusterClient) -> Self {
        Self::cluster(client)
    }
}

impl RedisRuntimeClient {
    pub fn direct(client: redis::Client) -> Self {
        Self::Direct(RedisDirectRuntimeClient::new(client))
    }

    pub fn direct_with_timeouts(client: redis::Client, timeouts: RedisRuntimeTimeouts) -> Self {
        Self::Direct(RedisDirectRuntimeClient::with_timeouts(client, timeouts))
    }

    pub fn failover(client: redis::sentinel::SentinelClient) -> Self {
        Self::Failover(RedisFailoverRuntimeClient::new(client))
    }

    pub fn failover_with_timeouts(
        client: redis::sentinel::SentinelClient,
        timeouts: RedisRuntimeTimeouts,
    ) -> Self {
        Self::Failover(RedisFailoverRuntimeClient::with_timeouts(client, timeouts))
    }

    pub fn cluster(client: redis::cluster::ClusterClient) -> Self {
        Self::Cluster(RedisClusterRuntimeClient::new(client))
    }

    pub fn cluster_with_timeouts(
        client: redis::cluster::ClusterClient,
        timeouts: RedisRuntimeTimeouts,
    ) -> Self {
        Self::Cluster(RedisClusterRuntimeClient::with_timeouts(client, timeouts))
    }

    pub fn cluster_with_timeouts_and_max_redirects(
        client: redis::cluster::ClusterClient,
        timeouts: RedisRuntimeTimeouts,
        max_redirects: u32,
    ) -> Self {
        Self::Cluster(RedisClusterRuntimeClient::with_timeouts_and_max_redirects(
            client,
            timeouts,
            Some(max_redirects),
        ))
    }

    pub fn as_direct_client(&self) -> Option<&redis::Client> {
        match self {
            Self::Direct(client) => Some(client.client()),
            Self::Failover(_) | Self::Cluster(_) => None,
        }
    }

    pub fn into_direct_client(self) -> Result<redis::Client, MakeRedisClientError> {
        match self {
            Self::Direct(client) => Ok(client.into_client()),
            Self::Failover(_) => Err(MakeRedisClientError::UnsupportedConnectionOption(
                "failover".to_owned(),
            )),
            Self::Cluster(_) => Err(MakeRedisClientError::UnsupportedConnectionOption(
                "cluster".to_owned(),
            )),
        }
    }

    pub async fn into_runtime_connection(
        self,
    ) -> Result<RedisRuntimeConnection, MakeRedisClientError> {
        match self {
            Self::Direct(client) => client
                .client()
                .get_multiplexed_async_connection_with_config(&client.async_connection_config())
                .await
                .map(RedisRuntimeConnection::Direct)
                .map_err(|error| MakeRedisClientError::Redis(error.to_string())),
            Self::Failover(client) => client
                .client()
                .lock()
                .await
                .get_async_connection_with_config(&client.async_connection_config())
                .await
                .map(RedisRuntimeConnection::Direct)
                .map_err(|error| MakeRedisClientError::Redis(error.to_string())),
            Self::Cluster(client) => client
                .client()
                .get_async_connection_with_config(client.cluster_config())
                .await
                .map(RedisRuntimeConnection::Cluster)
                .map_err(|error| MakeRedisClientError::Redis(error.to_string())),
        }
    }
}

fn asynq_timeout_or_default(duration: Duration, default: Duration) -> Duration {
    if duration.is_zero() {
        default
    } else {
        duration
    }
}

#[derive(Clone)]
pub enum RedisRuntimeConnection {
    Direct(redis::aio::MultiplexedConnection),
    Cluster(redis::cluster_async::ClusterConnection),
}

impl fmt::Debug for RedisRuntimeConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Direct(_) => f
                .debug_tuple("Direct")
                .field(&"MultiplexedConnection")
                .finish(),
            Self::Cluster(_) => f
                .debug_tuple("Cluster")
                .field(&"ClusterConnection")
                .finish(),
        }
    }
}

impl redis::aio::ConnectionLike for RedisRuntimeConnection {
    fn req_packed_command<'a>(
        &'a mut self,
        cmd: &'a redis::Cmd,
    ) -> redis::RedisFuture<'a, redis::Value> {
        async move {
            match self {
                Self::Direct(connection) => connection.req_packed_command(cmd).await,
                Self::Cluster(connection) => connection.req_packed_command(cmd).await,
            }
        }
        .boxed()
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
        async move {
            match self {
                Self::Direct(connection) => {
                    connection.req_packed_commands(cmd, offset, count).await
                }
                Self::Cluster(connection) => {
                    connection.req_packed_commands(cmd, offset, count).await
                }
            }
        }
        .boxed()
    }

    fn get_db(&self) -> i64 {
        match self {
            Self::Direct(connection) => connection.get_db(),
            Self::Cluster(connection) => connection.get_db(),
        }
    }
}
