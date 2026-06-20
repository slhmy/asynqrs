//! Redis-backed public server constructors.
//!
//! Reference: Asynq v0.26.0 `NewServer` and `NewServerFromRedisClient`
//! construct Redis-backed server components from connection options or an
//! existing Redis client:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L311-L620>.

use crate::SystemClock;
use crate::broker::redis::{
    RedisCancelListener, RedisRuntimeBroker, RedisRuntimeClient, redis_broker_from_runtime_client,
};
use crate::processing::{
    DefaultIsFailure, DefaultRetryDelay, NoopErrorHandler, NoopLeaseExtender, TaskExecutor,
};

use super::super::{
    Config, Server, ServerConstructionError, ServerError, ServerHandle, ServerRunSummary,
    TokioSleeper, WorkerAssembly,
};

type RedisBackedWorkerAssembly<H> = WorkerAssembly<
    RedisRuntimeBroker,
    H,
    DefaultRetryDelay,
    SystemClock,
    DefaultIsFailure,
    NoopErrorHandler,
    NoopLeaseExtender,
>;
type RedisBackedServerInner<H> = Server<RedisBackedWorkerAssembly<H>, TokioSleeper>;

/// Concrete server type backed by a Redis multiplexed connection.
///
/// Reference: Asynq v0.26.0 public `Server` created by
/// `NewServerFromRedisClient`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L431-L620>.
#[derive(Debug, Clone)]
pub struct RedisBackedServer<H> {
    inner: RedisBackedServerInner<H>,
}

impl<H> RedisBackedServer<H> {
    pub(crate) fn new(inner: RedisBackedServerInner<H>) -> Self {
        Self { inner }
    }

    pub async fn run(&mut self) -> Result<ServerRunSummary, ServerError>
    where
        H: crate::Handler + Send,
    {
        self.inner.run().await
    }

    pub fn start(self) -> Result<ServerHandle, ServerError>
    where
        H: crate::Handler + Clone + Send + 'static,
    {
        self.inner.start()
    }

    pub async fn ping(&mut self) -> Result<(), ServerError>
    where
        H: crate::Handler + Clone + Send,
    {
        self.inner.ping().await
    }
}

/// Redis-backed server constructor returned before a task handler is installed.
///
/// Reference: Asynq v0.26.0 `NewServer` constructs a server first, then
/// `Server.Run`/`Server.Start` receive the handler:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L311-L721>.
#[derive(Debug, Clone)]
pub struct RedisBackedServerBuilder {
    redis_client: RedisRuntimeClient,
    config: Config,
    pub(crate) shared_connection: bool,
}

impl RedisBackedServerBuilder {
    /// Creates a Redis-backed server builder that owns the runtime client.
    ///
    /// Most applications should use `from_redis_client` or
    /// `from_redis_runtime_client`, which model caller-owned Redis handles.
    /// Use this constructor when the server should own and close the supplied
    /// `RedisRuntimeClient` during shutdown.
    ///
    /// Reference: Asynq v0.26.0 `NewServer` constructs an owned Redis-backed
    /// server from Redis options:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L311-L429>.
    pub fn new(redis_client: RedisRuntimeClient, config: Config) -> Self {
        Self {
            redis_client,
            config,
            shared_connection: false,
        }
    }

    /// Creates a Redis-backed server builder backed by caller-owned resources.
    ///
    /// Reference: Asynq v0.26.0 `NewServerFromRedisClient` uses a
    /// caller-owned Redis client and does not close it during shutdown:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L431-L620>.
    pub fn with_shared_connection(redis_client: RedisRuntimeClient, config: Config) -> Self {
        Self {
            redis_client,
            config,
            shared_connection: true,
        }
    }

    pub fn from_redis_client(redis_client: impl Into<RedisRuntimeClient>, config: Config) -> Self {
        Self::from_redis_runtime_client(redis_client.into(), config)
    }

    pub fn from_redis_runtime_client(redis_client: RedisRuntimeClient, config: Config) -> Self {
        Self::with_shared_connection(redis_client, config)
    }

    pub fn from_direct_redis_client(redis_client: redis::Client, config: Config) -> Self {
        Self::from_redis_runtime_client(RedisRuntimeClient::direct(redis_client), config)
    }

    pub async fn build_with_handler<H>(
        self,
        handler: H,
    ) -> Result<RedisBackedServer<H>, ServerConstructionError>
    where
        H: crate::Handler + Send,
    {
        build_redis_backed_server(
            self.redis_client,
            self.config,
            self.shared_connection,
            handler,
        )
        .await
    }

    pub async fn run<H>(self, handler: H) -> Result<ServerRunSummary, ServerConstructionError>
    where
        H: crate::Handler + Send,
    {
        let mut server = self.build_with_handler(handler).await?;
        Ok(server.run().await?)
    }

    pub async fn start<H>(self, handler: H) -> Result<ServerHandle, ServerConstructionError>
    where
        H: crate::Handler + Clone + Send + 'static,
    {
        Ok(self.build_with_handler(handler).await?.start()?)
    }
}

async fn build_redis_backed_server<H>(
    redis_client: RedisRuntimeClient,
    config: Config,
    shared_connection: bool,
    handler: H,
) -> Result<RedisBackedServer<H>, ServerConstructionError>
where
    H: crate::Handler + Send,
{
    let broker = redis_broker_from_runtime_client(redis_client.clone()).await?;
    let aggregation_broker = broker.clone();
    let runtime = WorkerAssembly::new(
        broker,
        TaskExecutor::new(handler, DefaultRetryDelay, DefaultIsFailure),
        SystemClock,
    )
    .apply_server_config(&config);
    let server = Server::with_config_and_aggregation_broker(
        runtime,
        config,
        TokioSleeper,
        aggregation_broker,
        TokioSleeper,
    )?;
    let server = if shared_connection {
        server.with_shared_connection()
    } else {
        server
    };
    let listener = RedisCancelListener::from_runtime_state(redis_client, server.runtime_state());
    Ok(RedisBackedServer::new(
        server.with_cancellation_listener(listener),
    ))
}
