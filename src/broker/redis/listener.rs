use futures_util::StreamExt as _;
use thiserror::Error;
use tokio::sync::{mpsc, watch};

use crate::server::CancellationListener;
use crate::server::ServerRuntimeState;
use crate::{ServerCanceller, ServerError};

use super::{
    RedisClusterRuntimeClient, RedisDirectRuntimeClient, RedisFailoverRuntimeClient,
    RedisRuntimeClient, keys,
};

/// Redis pub/sub listener for active task cancellation.
///
/// Reference: Asynq v0.26.0 processor subscribes to `asynq:cancel` and cancels
/// matching active task contexts:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go>.
///
#[derive(Debug, Clone)]
pub struct RedisCancelListener {
    client: RedisRuntimeClient,
    canceller: ServerCanceller,
}

#[derive(Debug, Error)]
pub enum RedisCancelListenerError {
    #[error("Redis pub/sub failed: {0}")]
    Redis(#[from] redis::RedisError),
}

enum RedisCancellationSubscription {
    Direct(redis::aio::PubSubStream),
    Push {
        _connection: RedisPushConnection,
        messages: mpsc::UnboundedReceiver<redis::PushInfo>,
    },
}

enum RedisPushConnection {
    Failover {
        _connection: redis::aio::MultiplexedConnection,
    },
    Cluster {
        _connection: redis::cluster_async::ClusterConnection,
    },
}

impl RedisCancellationSubscription {
    async fn next_task_id(&mut self) -> Result<Option<String>, RedisCancelListenerError> {
        loop {
            match self {
                Self::Direct(messages) => {
                    return messages
                        .next()
                        .await
                        .map(|message| {
                            message
                                .get_payload()
                                .map_err(RedisCancelListenerError::from)
                        })
                        .transpose();
                }
                Self::Push { messages, .. } => {
                    let Some(message) = messages.recv().await else {
                        return Ok(None);
                    };
                    if let Some(task_id) = cancellation_payload(message)? {
                        return Ok(Some(task_id));
                    }
                }
            }
        }
    }
}

impl RedisCancelListener {
    pub fn new(client: impl Into<RedisRuntimeClient>, canceller: ServerCanceller) -> Self {
        Self {
            client: client.into(),
            canceller,
        }
    }

    pub fn from_runtime_state(
        client: impl Into<RedisRuntimeClient>,
        runtime: ServerRuntimeState,
    ) -> Self {
        Self::new(client, runtime.canceller())
    }

    pub async fn run_until_stopped(
        &mut self,
        shutdown: watch::Receiver<bool>,
    ) -> Result<usize, RedisCancelListenerError> {
        let Some(mut subscription) = self.subscribe_until_ready(shutdown.clone()).await? else {
            return Ok(0);
        };
        self.run_subscription_until_stopped(&mut subscription, shutdown)
            .await
    }

    async fn run_subscription_until_stopped(
        &self,
        subscription: &mut RedisCancellationSubscription,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<usize, RedisCancelListenerError> {
        let mut canceled = 0;

        loop {
            tokio::select! {
                task_id = subscription.next_task_id() => {
                    let Some(task_id) = task_id? else {
                        break;
                    };
                    if self.canceller.cancel(&task_id) {
                        canceled += 1;
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
        }

        Ok(canceled)
    }

    async fn subscribe_until_ready(
        &self,
        shutdown: watch::Receiver<bool>,
    ) -> Result<Option<RedisCancellationSubscription>, RedisCancelListenerError> {
        match self.client.clone() {
            RedisRuntimeClient::Direct(client) => {
                self.subscribe_direct_until_ready(client, shutdown).await
            }
            RedisRuntimeClient::Failover(client) => {
                self.subscribe_failover_until_ready(client, shutdown).await
            }
            RedisRuntimeClient::Cluster(client) => {
                self.subscribe_cluster_until_ready(client, shutdown).await
            }
        }
    }

    async fn subscribe_direct_until_ready(
        &self,
        client: RedisDirectRuntimeClient,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<Option<RedisCancellationSubscription>, RedisCancelListenerError> {
        loop {
            if *shutdown.borrow() {
                return Ok(None);
            }
            match client.client().get_async_pubsub().await {
                Ok(mut pubsub) => match pubsub.subscribe(keys::CANCEL_CHANNEL).await {
                    Ok(()) => {
                        return Ok(Some(RedisCancellationSubscription::Direct(
                            pubsub.into_on_message(),
                        )));
                    }
                    Err(_error) => {
                        subscribe_retry_delay(&mut shutdown).await;
                    }
                },
                Err(_error) => {
                    subscribe_retry_delay(&mut shutdown).await;
                }
            }
        }
    }

    async fn subscribe_failover_until_ready(
        &self,
        client: RedisFailoverRuntimeClient,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<Option<RedisCancellationSubscription>, RedisCancelListenerError> {
        loop {
            if *shutdown.borrow() {
                return Ok(None);
            }
            let (tx, messages) = mpsc::unbounded_channel();
            let mut config = client.timeouts().async_connection_config();
            config = config.set_push_sender(tx);
            let connection = client
                .client()
                .lock()
                .await
                .get_async_connection_with_config(&config)
                .await;
            match connection {
                Ok(mut connection) => match connection.subscribe(keys::CANCEL_CHANNEL).await {
                    Ok(()) => {
                        return Ok(Some(RedisCancellationSubscription::Push {
                            _connection: RedisPushConnection::Failover {
                                _connection: connection,
                            },
                            messages,
                        }));
                    }
                    Err(_error) => {
                        subscribe_retry_delay(&mut shutdown).await;
                    }
                },
                Err(_error) => {
                    subscribe_retry_delay(&mut shutdown).await;
                }
            }
        }
    }

    async fn subscribe_cluster_until_ready(
        &self,
        client: RedisClusterRuntimeClient,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<Option<RedisCancellationSubscription>, RedisCancelListenerError> {
        loop {
            if *shutdown.borrow() {
                return Ok(None);
            }
            let (tx, messages) = mpsc::unbounded_channel();
            let mut config = client.timeouts().cluster_config();
            config = config.set_push_sender(tx);
            let connection = client
                .client()
                .get_async_connection_with_config(config)
                .await;
            match connection {
                Ok(mut connection) => match connection.subscribe(keys::CANCEL_CHANNEL).await {
                    Ok(()) => {
                        return Ok(Some(RedisCancellationSubscription::Push {
                            _connection: RedisPushConnection::Cluster {
                                _connection: connection,
                            },
                            messages,
                        }));
                    }
                    Err(_error) => {
                        subscribe_retry_delay(&mut shutdown).await;
                    }
                },
                Err(_error) => {
                    subscribe_retry_delay(&mut shutdown).await;
                }
            }
        }
    }
}

fn cancellation_payload(message: redis::PushInfo) -> Result<Option<String>, redis::RedisError> {
    use redis::FromRedisValue as _;

    if message.kind != redis::PushKind::Message || message.data.len() < 2 {
        return Ok(None);
    }
    let channel = String::from_redis_value(message.data[0].clone())?;
    if channel != keys::CANCEL_CHANNEL {
        return Ok(None);
    }
    Ok(Some(String::from_redis_value(message.data[1].clone())?))
}

async fn subscribe_retry_delay(shutdown: &mut watch::Receiver<bool>) {
    // Reference: Asynq v0.26.0 subscriber retries Redis cancellation-channel
    // subscription every 5 seconds until shutdown:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/subscriber.go#L57-L73>.
    tokio::select! {
        _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
        changed = shutdown.changed() => {
            let _ = changed;
        }
    }
}

impl CancellationListener for RedisCancelListener {
    fn run_until_stopped(
        &self,
        shutdown: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<Result<usize, ServerError>> {
        let mut listener = self.clone();
        tokio::spawn(async move {
            RedisCancelListener::run_until_stopped(&mut listener, shutdown)
                .await
                .map_err(|error| ServerError::CancellationListener(error.to_string()))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::IntoConnectionInfo;

    #[test]
    fn redis_cancel_listener_accepts_runtime_client_boundary() {
        let client = redis::Client::open("redis://localhost:6379").unwrap();
        let canceller = ServerCanceller::default();
        let _listener =
            RedisCancelListener::new(RedisRuntimeClient::direct(client), canceller.clone());

        assert!(!canceller.cancel("task-id"));
    }

    #[test]
    fn redis_cancel_listener_accepts_server_runtime_state() {
        let client = redis::Client::open("redis://localhost:6379").unwrap();
        let runtime = ServerRuntimeState::default();
        let canceller = runtime.canceller();
        let _listener =
            RedisCancelListener::from_runtime_state(RedisRuntimeClient::direct(client), runtime);

        assert!(!canceller.cancel("task-id"));
    }

    #[tokio::test]
    async fn failover_listener_stops_cleanly_during_subscribe_retry() {
        let client = redis::sentinel::SentinelClientBuilder::new(
            vec![redis::ConnectionAddr::Tcp("127.0.0.1".to_owned(), 1)],
            "mymaster",
            redis::sentinel::SentinelServerType::Master,
        )
        .unwrap()
        .build()
        .unwrap();
        let mut listener = RedisCancelListener::new(
            RedisRuntimeClient::failover(client),
            ServerCanceller::default(),
        );
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        shutdown_tx.send(true).unwrap();

        assert_eq!(
            RedisCancelListener::run_until_stopped(&mut listener, shutdown_rx)
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn cluster_listener_stops_cleanly_during_subscribe_retry() {
        let client = redis::cluster::ClusterClient::builder(vec![
            "redis://127.0.0.1:1".into_connection_info().unwrap(),
        ])
        .build()
        .unwrap();
        let mut listener = RedisCancelListener::new(
            RedisRuntimeClient::cluster(client),
            ServerCanceller::default(),
        );
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        shutdown_tx.send(true).unwrap();

        assert_eq!(
            RedisCancelListener::run_until_stopped(&mut listener, shutdown_rx)
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn direct_listener_retries_subscribe_errors_until_shutdown_like_upstream() {
        let client = redis::Client::open("redis://127.0.0.1:1/").unwrap();
        let mut listener = RedisCancelListener::new(client, crate::ServerCanceller::default());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let run = tokio::spawn(async move {
            RedisCancelListener::run_until_stopped(&mut listener, shutdown_rx).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        shutdown_tx.send(true).unwrap();

        assert_eq!(run.await.unwrap().unwrap(), 0);
    }

    #[test]
    fn cancellation_payload_reads_resp3_pubsub_message() {
        let message = redis::PushInfo {
            kind: redis::PushKind::Message,
            data: vec![
                redis::Value::BulkString(keys::CANCEL_CHANNEL.as_bytes().to_vec()),
                redis::Value::BulkString(b"task-123".to_vec()),
            ],
        };

        assert_eq!(
            cancellation_payload(message).unwrap(),
            Some("task-123".to_owned())
        );
    }

    #[test]
    fn cancellation_payload_ignores_non_cancel_pushes() {
        let subscription = redis::PushInfo {
            kind: redis::PushKind::Subscribe,
            data: vec![
                redis::Value::BulkString(keys::CANCEL_CHANNEL.as_bytes().to_vec()),
                redis::Value::Int(1),
            ],
        };
        let other_channel = redis::PushInfo {
            kind: redis::PushKind::Message,
            data: vec![
                redis::Value::BulkString(b"other".to_vec()),
                redis::Value::BulkString(b"task-123".to_vec()),
            ],
        };

        assert_eq!(cancellation_payload(subscription).unwrap(), None);
        assert_eq!(cancellation_payload(other_channel).unwrap(), None);
    }
}
