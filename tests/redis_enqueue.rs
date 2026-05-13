use std::collections::HashMap;
use std::time::Duration;

use asynq_rs::{
    BrokerError, Client, ClientError, RedisBroker, RedisConnectionExecutor, RedisEnqueueScript,
    RedisScriptResult, Task, TaskMessage, TaskOption, TaskState,
};
use redis::Commands;
use testcontainers_modules::{
    redis::{REDIS_PORT, Redis},
    testcontainers::{Container, runners::SyncRunner},
};

const REDIS_URL_ENV: &str = "ASYNQ_RS_REDIS_URL";

// Reference: Asynq v0.26.0 Redis enqueue scripts and key layout:
// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L735>.

#[test]
fn pending_enqueue_writes_task_hash_pending_list_and_queue_set() {
    let Some(mut fixture) = RedisFixture::new("pending") else {
        return;
    };
    let mut client = fixture.client();
    let task = Task::new_with_options(
        "email:welcome",
        b"payload".to_vec(),
        [
            TaskOption::queue(fixture.queue()),
            TaskOption::task_id("task-id"),
        ],
    );

    let result = client.enqueue(&task).unwrap();

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

    let ids: Vec<String> = fixture
        .connection
        .lrange(fixture.pending_key(), 0, -1)
        .unwrap();
    assert_eq!(ids, ["task-id"]);
    let queue = fixture.queue().to_owned();
    assert!(
        fixture
            .connection
            .sismember::<_, _, bool>("asynq:queues", queue)
            .unwrap()
    );
}

#[test]
fn scheduled_enqueue_writes_task_hash_and_scheduled_zset() {
    let Some(mut fixture) = RedisFixture::new("scheduled") else {
        return;
    };
    let mut client = fixture.client();
    let task = Task::new_with_options(
        "email:welcome",
        b"payload".to_vec(),
        [
            TaskOption::queue(fixture.queue()),
            TaskOption::task_id("task-id"),
            TaskOption::process_in(Duration::from_secs(60)),
        ],
    );

    let result = client.enqueue(&task).unwrap();

    assert_eq!(result.state(), TaskState::Scheduled);

    let task_key = fixture.task_key("task-id");
    let stored: HashMap<String, Vec<u8>> = fixture.connection.hgetall(&task_key).unwrap();
    assert_eq!(string_field(&stored, "state"), "scheduled");
    assert_eq!(
        decode_msg(stored.get("msg").unwrap()).queue,
        fixture.queue().to_owned()
    );

    let score: f64 = fixture
        .connection
        .zscore(fixture.scheduled_key(), "task-id")
        .unwrap();
    let process_at = result.next_process_at().unwrap();
    let expected = process_at
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as f64;
    assert_eq!(score, expected);
}

#[test]
fn unique_enqueue_sets_unique_key_and_rejects_duplicate() {
    let Some(mut fixture) = RedisFixture::new("unique") else {
        return;
    };
    let mut client = fixture.client();
    let options = [
        TaskOption::queue(fixture.queue()),
        TaskOption::task_id("task-id"),
        TaskOption::unique(Duration::from_secs(300)),
    ];
    let task = Task::new_with_options("email:welcome", b"payload".to_vec(), options.clone());

    client.enqueue(&task).unwrap();
    let duplicate = Task::new_with_options(
        "email:welcome",
        b"payload".to_vec(),
        [
            TaskOption::queue(fixture.queue()),
            TaskOption::task_id("second-id"),
            TaskOption::unique(Duration::from_secs(300)),
        ],
    );
    let error = client.enqueue(&duplicate).unwrap_err();

    assert_eq!(error, ClientError::Broker(BrokerError::DuplicateTask));

    let unique_key = fixture.unique_key("email:welcome", b"payload");
    let lock_owner: String = fixture.connection.get(&unique_key).unwrap();
    let ttl: i64 = fixture.connection.ttl(&unique_key).unwrap();
    let task_hash_unique_key: String = fixture
        .connection
        .hget(fixture.task_key("task-id"), "unique_key")
        .unwrap();

    assert_eq!(lock_owner, "task-id");
    assert_eq!(task_hash_unique_key, unique_key);
    assert!(ttl > 0 && ttl <= 300);
}

#[test]
fn group_enqueue_writes_group_zset_and_groups_set() {
    let Some(mut fixture) = RedisFixture::new("group") else {
        return;
    };
    let mut client = fixture.client();
    let task = Task::new_with_options(
        "email:welcome",
        b"payload".to_vec(),
        [
            TaskOption::queue(fixture.queue()),
            TaskOption::task_id("task-id"),
            TaskOption::group("tenant-a"),
        ],
    );

    let result = client.enqueue(&task).unwrap();

    assert_eq!(result.state(), TaskState::Aggregating);

    let task_key = fixture.task_key("task-id");
    let stored: HashMap<String, Vec<u8>> = fixture.connection.hgetall(&task_key).unwrap();
    assert_eq!(string_field(&stored, "state"), "aggregating");
    assert_eq!(string_field(&stored, "group"), "tenant-a");

    let score: f64 = fixture
        .connection
        .zscore(fixture.group_key("tenant-a"), "task-id")
        .unwrap();
    assert!(score > 0.0);
    let groups_key = fixture.groups_key();
    assert!(
        fixture
            .connection
            .sismember::<_, _, bool>(groups_key, "tenant-a")
            .unwrap()
    );
}

#[test]
fn script_result_mapping_documents_unique_duplicate_code() {
    assert_eq!(
        RedisEnqueueScript::EnqueueUnique.result_for_code(-1),
        Some(RedisScriptResult::DuplicateTask)
    );
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

    fn client(&self) -> Client<RedisBroker<RedisConnectionExecutor<redis::Connection>>> {
        let redis_client = redis::Client::open(self.url.as_ref()).unwrap();
        let connection = redis_client.get_connection().unwrap();
        let executor = RedisConnectionExecutor::new(connection);
        Client::new(RedisBroker::new(executor))
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

    fn scheduled_key(&self) -> String {
        format!("asynq:{{{}}}:scheduled", self.queue)
    }

    fn group_key(&self, group: &str) -> String {
        format!("asynq:{{{}}}:g:{group}", self.queue)
    }

    fn groups_key(&self) -> String {
        format!("asynq:{{{}}}:groups", self.queue)
    }

    fn unique_key(&self, task_type: &str, payload: &[u8]) -> String {
        use md5::{Digest, Md5};
        let checksum = Md5::digest(payload);
        format!("asynq:{{{}}}:unique:{task_type}:{checksum:x}", self.queue)
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
