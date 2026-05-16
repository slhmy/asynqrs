use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use asynq_rs::{
    AsyncCompleteBroker, AsyncDequeueBroker, AsyncHandler, AsyncProcessor, AsyncRedisBroker,
    AsyncRedisConnectionExecutor, AsyncRetryBroker, AsyncServer, EnqueuePlan, EnqueueResult,
    HandlerError, RedisScript, RedisScriptResult, Task, TaskMessage, TaskOption, TaskState,
};
use redis::Commands;
use testcontainers_modules::{
    redis::{REDIS_PORT, Redis},
    testcontainers::{Container, runners::SyncRunner},
};
use tokio::sync::{oneshot, watch};

const REDIS_URL_ENV: &str = "ASYNQ_RS_REDIS_URL";

// Reference: Asynq v0.26.0 Redis task scripts and key layout:
// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L735>.

#[test]
fn async_pending_enqueue_dequeue_and_complete_uses_redis_layout() {
    let Some(mut fixture) = RedisFixture::new("async-pending-complete") else {
        return;
    };
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async_pending_enqueue_dequeue_and_complete_uses_redis_layout_inner(&mut fixture));
}

async fn async_pending_enqueue_dequeue_and_complete_uses_redis_layout_inner(
    fixture: &mut RedisFixture,
) {
    let task = Task::new_with_options(
        "email:welcome",
        b"payload".to_vec(),
        [
            TaskOption::queue(fixture.queue()),
            TaskOption::task_id("task-id"),
            TaskOption::retention(Duration::from_secs(300)),
        ],
    );

    let result = fixture.enqueue(&task).await;

    assert_eq!(result.id(), "task-id");
    assert_eq!(result.queue(), fixture.queue());
    assert_eq!(result.state(), TaskState::Pending);
    let task_key = fixture.task_key("task-id");
    let stored: HashMap<String, Vec<u8>> = fixture.connection.hgetall(&task_key).unwrap();
    assert_eq!(string_field(&stored, "state"), "pending");
    assert!(stored.contains_key("pending_since"));
    assert_eq!(
        decode_msg(stored.get("msg").unwrap()).r#type,
        "email:welcome"
    );

    let mut broker = fixture.async_broker().await;
    let dequeued = broker.dequeue(&[fixture.queue().to_owned()]).await.unwrap();
    assert_eq!(dequeued.message().id, "task-id");
    assert_eq!(dequeued.message().queue, fixture.queue());
    let stored: HashMap<String, Vec<u8>> = fixture.connection.hgetall(&task_key).unwrap();
    assert_eq!(string_field(&stored, "state"), "active");
    let lease_score: f64 = fixture
        .connection
        .zscore(fixture.lease_key(), "task-id")
        .unwrap();
    assert!(lease_score > 0.0);

    broker.complete(dequeued.message()).await.unwrap();

    let stored: HashMap<String, Vec<u8>> = fixture.connection.hgetall(&task_key).unwrap();
    assert_eq!(string_field(&stored, "state"), "completed");
    let completed_msg = decode_msg(stored.get("msg").unwrap());
    assert!(completed_msg.completed_at > 0);
    let completed_score: f64 = fixture
        .connection
        .zscore(fixture.completed_key(), "task-id")
        .unwrap();
    assert_eq!(completed_score as i64, completed_msg.completed_at + 300);
    let active_ids: Vec<String> = fixture
        .connection
        .lrange(fixture.active_key(), 0, -1)
        .unwrap();
    assert!(active_ids.is_empty());
}

#[test]
fn async_retry_records_failure_and_moves_task_to_retry_set() {
    let Some(mut fixture) = RedisFixture::new("async-retry") else {
        return;
    };
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async_retry_records_failure_and_moves_task_to_retry_set_inner(&mut fixture));
}

async fn async_retry_records_failure_and_moves_task_to_retry_set_inner(fixture: &mut RedisFixture) {
    let task = Task::new_with_options(
        "email:welcome",
        b"payload".to_vec(),
        [
            TaskOption::queue(fixture.queue()),
            TaskOption::task_id("task-id"),
            TaskOption::max_retry(5),
        ],
    );

    fixture.enqueue(&task).await;
    let mut broker = fixture.async_broker().await;
    let dequeued = broker.dequeue(&[fixture.queue().to_owned()]).await.unwrap();
    broker
        .retry(
            dequeued.message(),
            SystemTime::now() + Duration::from_secs(60),
            "handler failed",
            true,
        )
        .await
        .unwrap();

    let retry_score: f64 = fixture
        .connection
        .zscore(fixture.retry_key(), "task-id")
        .unwrap();
    assert!(retry_score > 0.0);
    let stored: HashMap<String, Vec<u8>> = fixture
        .connection
        .hgetall(fixture.task_key("task-id"))
        .unwrap();
    assert_eq!(string_field(&stored, "state"), "retry");
    let retry_msg = decode_msg(stored.get("msg").unwrap());
    assert_eq!(retry_msg.retried, 1);
    assert_eq!(retry_msg.error_msg, "handler failed");
}

#[test]
fn async_server_with_redis_processor_completes_task_and_stops() {
    let Some(mut fixture) = RedisFixture::new("async-server-complete") else {
        return;
    };
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async_server_with_redis_processor_completes_task_and_stops_inner(&mut fixture));
}

async fn async_server_with_redis_processor_completes_task_and_stops_inner(
    fixture: &mut RedisFixture,
) {
    let task = Task::new_with_options(
        "email:welcome",
        b"payload".to_vec(),
        [
            TaskOption::queue(fixture.queue()),
            TaskOption::task_id("task-id"),
            TaskOption::retention(Duration::from_secs(300)),
        ],
    );

    fixture.enqueue(&task).await;
    let broker = fixture.async_broker().await;
    let processor = AsyncProcessor::new(broker, |task: &Task| {
        assert_eq!(task.type_name(), "email:welcome");
        Ok::<(), HandlerError>(())
    });
    let mut server = AsyncServer::new(processor, [fixture.queue().to_owned()]).unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle = tokio::spawn(async move { server.run_until_stopped(shutdown_rx).await });

    wait_for_state(fixture, "task-id", "completed").await;
    shutdown_tx.send(true).unwrap();
    let summary = tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(summary.processed(), 1);
    assert_eq!(summary.completed(), 1);
    let active_ids: Vec<String> = fixture
        .connection
        .lrange(fixture.active_key(), 0, -1)
        .unwrap();
    assert!(active_ids.is_empty());
    let completed_score: f64 = fixture
        .connection
        .zscore(fixture.completed_key(), "task-id")
        .unwrap();
    assert!(completed_score > 0.0);
}

#[test]
fn async_server_shutdown_requeues_in_flight_task() {
    let Some(mut fixture) = RedisFixture::new("async-server-requeue") else {
        return;
    };
    tokio::runtime::Runtime::new().unwrap().block_on(
        async_server_shutdown_requeues_in_flight_task_inner(&mut fixture),
    );
}

async fn async_server_shutdown_requeues_in_flight_task_inner(fixture: &mut RedisFixture) {
    let task = Task::new_with_options(
        "email:shutdown",
        b"payload".to_vec(),
        [
            TaskOption::queue(fixture.queue()),
            TaskOption::task_id("shutdown-id"),
        ],
    );

    fixture.enqueue(&task).await;
    let broker = fixture.async_broker().await;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (started_tx, started_rx) = oneshot::channel();
    let processor = AsyncProcessor::new(
        broker,
        BlockingAsyncHandler {
            started_tx: Some(started_tx),
        },
    );
    let mut server = AsyncServer::new(processor, [fixture.queue().to_owned()]).unwrap();
    let handle = tokio::spawn(async move { server.run_until_stopped(shutdown_rx).await });

    started_rx.await.unwrap();
    shutdown_tx.send(true).unwrap();
    let summary = tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(summary.processed(), 0);
    assert_eq!(summary.completed(), 0);
    let stored: HashMap<String, Vec<u8>> = fixture
        .connection
        .hgetall(fixture.task_key("shutdown-id"))
        .unwrap();
    assert_eq!(string_field(&stored, "state"), "pending");
    let active_ids: Vec<String> = fixture
        .connection
        .lrange(fixture.active_key(), 0, -1)
        .unwrap();
    assert!(active_ids.is_empty());
    let pending_ids: Vec<String> = fixture
        .connection
        .lrange(fixture.pending_key(), 0, -1)
        .unwrap();
    assert_eq!(pending_ids, ["shutdown-id"]);
}

#[test]
fn script_result_mapping_documents_unique_duplicate_code() {
    assert_eq!(
        RedisScript::EnqueueUnique.result_for_code(-1),
        Some(RedisScriptResult::DuplicateTask)
    );
}

struct BlockingAsyncHandler {
    started_tx: Option<oneshot::Sender<()>>,
}

#[async_trait::async_trait]
impl AsyncHandler for BlockingAsyncHandler {
    async fn process_task(&mut self, task: &Task) -> Result<(), HandlerError> {
        assert_eq!(task.type_name(), "email:shutdown");
        if let Some(sender) = self.started_tx.take() {
            sender.send(()).unwrap();
        }
        std::future::pending::<()>().await;
        Ok(())
    }
}

struct RedisFixture {
    _container: Option<Container<Redis>>,
    url: String,
    connection: redis::Connection,
    queue: String,
}

impl RedisFixture {
    fn new(name: &str) -> Option<Self> {
        let (url, container) = redis_url()?;
        let client = redis::Client::open(url.as_ref()).unwrap();
        let connection = client.get_connection().unwrap();
        let queue = format!("asynq-rs-test-{name}-{}", uuid::Uuid::new_v4().simple());
        let mut fixture = Self {
            _container: container,
            url,
            connection,
            queue,
        };
        fixture.cleanup();
        Some(fixture)
    }

    async fn enqueue(&self, task: &Task) -> EnqueueResult {
        let mut broker = self.async_broker().await;
        let plan =
            EnqueuePlan::from_task(task, SystemTime::now(), uuid::Uuid::new_v4().to_string())
                .unwrap();
        broker.enqueue(&plan).await.unwrap();
        EnqueueResult::from_plan(&plan)
    }

    async fn async_broker(
        &self,
    ) -> AsyncRedisBroker<AsyncRedisConnectionExecutor<redis::aio::MultiplexedConnection>> {
        let redis_client = redis::Client::open(self.url.as_ref()).unwrap();
        let connection = redis_client
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let executor = AsyncRedisConnectionExecutor::new(connection);
        AsyncRedisBroker::new(executor)
    }

    fn queue(&self) -> &str {
        &self.queue
    }

    fn task_key(&self, task_id: &str) -> String {
        format!("{}{}", self.task_key_prefix(), task_id)
    }

    fn task_key_prefix(&self) -> String {
        format!("asynq:{{{}}}:t:", self.queue)
    }

    fn pending_key(&self) -> String {
        format!("asynq:{{{}}}:pending", self.queue)
    }

    fn active_key(&self) -> String {
        format!("asynq:{{{}}}:active", self.queue)
    }

    fn lease_key(&self) -> String {
        format!("asynq:{{{}}}:lease", self.queue)
    }

    fn completed_key(&self) -> String {
        format!("asynq:{{{}}}:completed", self.queue)
    }

    fn retry_key(&self) -> String {
        format!("asynq:{{{}}}:retry", self.queue)
    }

    fn cleanup(&mut self) {
        let pattern = format!("asynq:{{{}}}:*", self.queue);
        let keys: Vec<String> = self.connection.keys(pattern).unwrap();
        if !keys.is_empty() {
            let _: usize = self.connection.del(keys).unwrap();
        }
        let _: usize = self.connection.srem("asynq:queues", &self.queue).unwrap();
    }
}

impl Drop for RedisFixture {
    fn drop(&mut self) {
        self.cleanup();
    }
}

async fn wait_for_state(fixture: &mut RedisFixture, task_id: &str, state: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let stored: HashMap<String, Vec<u8>> = fixture
            .connection
            .hgetall(fixture.task_key(task_id))
            .unwrap();
        if !stored.is_empty() && string_field(&stored, "state") == state {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {state}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn string_field(fields: &HashMap<String, Vec<u8>>, name: &str) -> String {
    String::from_utf8(fields.get(name).unwrap().clone()).unwrap()
}

fn decode_msg(data: &[u8]) -> TaskMessage {
    TaskMessage::decode_from_slice(data).unwrap()
}

fn redis_url() -> Option<(String, Option<Container<Redis>>)> {
    if let Ok(url) = std::env::var(REDIS_URL_ENV) {
        return Some((url, None));
    }

    let container = match Redis::default().start() {
        Ok(container) => container,
        Err(error) => {
            eprintln!(
                "skipping Redis integration test: set {REDIS_URL_ENV} or make Docker available ({error})"
            );
            return None;
        }
    };
    let host = match container.get_host() {
        Ok(host) => host,
        Err(error) => {
            eprintln!(
                "skipping Redis integration test: failed to resolve container host ({error})"
            );
            return None;
        }
    };
    let port = match container.get_host_port_ipv4(REDIS_PORT) {
        Ok(port) => port,
        Err(error) => {
            eprintln!("skipping Redis integration test: failed to resolve Redis port ({error})");
            return None;
        }
    };
    Some((format!("redis://{host}:{port}"), Some(container)))
}
