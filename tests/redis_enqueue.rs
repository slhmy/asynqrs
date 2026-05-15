use std::collections::HashMap;
use std::time::Duration;

use asynq_rs::{
    ArchiveBroker, BrokerError, Client, ClientError, CompleteBroker, DequeueBroker, ErrorHandler,
    ForwardBroker, HandlerError, LeaseBroker, Processor, ProcessorRun, RecoverBroker, RedisBroker,
    RedisConnectionExecutor, RedisScript, RedisScriptResult, RequeueBroker, RetryBroker, Task,
    TaskMessage, TaskOption, TaskState,
};
use redis::Commands;
use testcontainers_modules::{
    redis::{REDIS_PORT, Redis},
    testcontainers::{Container, runners::SyncRunner},
};

const REDIS_URL_ENV: &str = "ASYNQ_RS_REDIS_URL";

// Reference: Asynq v0.26.0 Redis task scripts and key layout:
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

    let dequeued = client
        .broker_mut()
        .dequeue(&[fixture.queue().to_owned()])
        .unwrap();
    assert_eq!(dequeued.message().id, "task-id");
    assert_eq!(dequeued.message().queue, fixture.queue());

    let stored: HashMap<String, Vec<u8>> = fixture.connection.hgetall(&task_key).unwrap();
    assert_eq!(string_field(&stored, "state"), "active");
    assert!(!stored.contains_key("pending_since"));

    let pending_ids: Vec<String> = fixture
        .connection
        .lrange(fixture.pending_key(), 0, -1)
        .unwrap();
    assert!(pending_ids.is_empty());
    let active_ids: Vec<String> = fixture
        .connection
        .lrange(fixture.active_key(), 0, -1)
        .unwrap();
    assert_eq!(active_ids, ["task-id"]);
    let lease_score: f64 = fixture
        .connection
        .zscore(fixture.lease_key(), "task-id")
        .unwrap();
    assert!(lease_score > 0.0);
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
fn complete_without_retention_deletes_task_and_releases_unique_lock() {
    let Some(mut fixture) = RedisFixture::new("complete-done") else {
        return;
    };
    let mut client = fixture.client();
    let task = Task::new_with_options(
        "email:welcome",
        b"payload".to_vec(),
        [
            TaskOption::queue(fixture.queue()),
            TaskOption::task_id("task-id"),
            TaskOption::unique(Duration::from_secs(300)),
        ],
    );

    client.enqueue(&task).unwrap();
    let unique_key = fixture.unique_key("email:welcome", b"payload");
    assert!(
        fixture
            .connection
            .exists::<_, bool>(unique_key.clone())
            .unwrap()
    );
    let dequeued = client
        .broker_mut()
        .dequeue(&[fixture.queue().to_owned()])
        .unwrap();

    client.broker_mut().complete(dequeued.message()).unwrap();

    assert!(
        !fixture
            .connection
            .exists::<_, bool>(fixture.task_key("task-id"))
            .unwrap()
    );
    let active_ids: Vec<String> = fixture
        .connection
        .lrange(fixture.active_key(), 0, -1)
        .unwrap();
    assert!(active_ids.is_empty());
    let lease_score: Option<f64> = fixture
        .connection
        .zscore(fixture.lease_key(), "task-id")
        .unwrap();
    assert!(lease_score.is_none());
    assert!(!fixture.connection.exists::<_, bool>(unique_key).unwrap());

    let processed_total: i64 = fixture
        .connection
        .get(fixture.processed_total_key())
        .unwrap();
    assert_eq!(processed_total, 1);
    let daily_keys: Vec<String> = fixture
        .connection
        .keys(fixture.processed_daily_key_pattern())
        .unwrap();
    assert_eq!(daily_keys.len(), 1);
    let processed_daily: i64 = fixture.connection.get(&daily_keys[0]).unwrap();
    assert_eq!(processed_daily, 1);
}

#[test]
fn complete_with_retention_moves_task_to_completed_set() {
    let Some(mut fixture) = RedisFixture::new("complete-retained") else {
        return;
    };
    let mut client = fixture.client();
    let task = Task::new_with_options(
        "email:welcome",
        b"payload".to_vec(),
        [
            TaskOption::queue(fixture.queue()),
            TaskOption::task_id("task-id"),
            TaskOption::retention(Duration::from_secs(300)),
        ],
    );

    client.enqueue(&task).unwrap();
    let dequeued = client
        .broker_mut()
        .dequeue(&[fixture.queue().to_owned()])
        .unwrap();

    client.broker_mut().complete(dequeued.message()).unwrap();

    let task_key = fixture.task_key("task-id");
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
    let lease_score: Option<f64> = fixture
        .connection
        .zscore(fixture.lease_key(), "task-id")
        .unwrap();
    assert!(lease_score.is_none());
}

#[test]
fn retry_moves_active_task_to_retry_set_and_records_failure_stats() {
    let Some(mut fixture) = RedisFixture::new("retry") else {
        return;
    };
    let mut client = fixture.client();
    let task = Task::new_with_options(
        "email:welcome",
        b"payload".to_vec(),
        [
            TaskOption::queue(fixture.queue()),
            TaskOption::task_id("task-id"),
            TaskOption::max_retry(5),
        ],
    );

    client.enqueue(&task).unwrap();
    let dequeued = client
        .broker_mut()
        .dequeue(&[fixture.queue().to_owned()])
        .unwrap();
    client
        .broker_mut()
        .retry(
            dequeued.message(),
            std::time::SystemTime::now() + Duration::from_secs(60),
            "handler failed",
            true,
        )
        .unwrap();

    let active_ids: Vec<String> = fixture
        .connection
        .lrange(fixture.active_key(), 0, -1)
        .unwrap();
    assert!(active_ids.is_empty());
    let lease_score: Option<f64> = fixture
        .connection
        .zscore(fixture.lease_key(), "task-id")
        .unwrap();
    assert!(lease_score.is_none());
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
    assert!(retry_msg.last_failed_at > 0);

    let processed_total: i64 = fixture
        .connection
        .get(fixture.processed_total_key())
        .unwrap();
    let failed_total: i64 = fixture.connection.get(fixture.failed_total_key()).unwrap();
    assert_eq!(processed_total, 1);
    assert_eq!(failed_total, 1);
    let processed_daily_keys: Vec<String> = fixture
        .connection
        .keys(fixture.processed_daily_key_pattern())
        .unwrap();
    let failed_daily_keys: Vec<String> = fixture
        .connection
        .keys(fixture.failed_daily_key_pattern())
        .unwrap();
    assert_eq!(processed_daily_keys.len(), 1);
    assert_eq!(failed_daily_keys.len(), 1);
}

#[test]
fn requeue_moves_active_task_back_to_pending_without_stats() {
    let Some(mut fixture) = RedisFixture::new("requeue") else {
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

    client.enqueue(&task).unwrap();
    let dequeued = client
        .broker_mut()
        .dequeue(&[fixture.queue().to_owned()])
        .unwrap();
    client.broker_mut().requeue(dequeued.message()).unwrap();

    let active_ids: Vec<String> = fixture
        .connection
        .lrange(fixture.active_key(), 0, -1)
        .unwrap();
    assert!(active_ids.is_empty());
    let lease_score: Option<f64> = fixture
        .connection
        .zscore(fixture.lease_key(), "task-id")
        .unwrap();
    assert!(lease_score.is_none());
    let pending_ids: Vec<String> = fixture
        .connection
        .lrange(fixture.pending_key(), 0, -1)
        .unwrap();
    assert_eq!(pending_ids, ["task-id"]);
    let stored: HashMap<String, Vec<u8>> = fixture
        .connection
        .hgetall(fixture.task_key("task-id"))
        .unwrap();
    assert_eq!(string_field(&stored, "state"), "pending");
    let processed_total: Option<i64> = fixture
        .connection
        .get(fixture.processed_total_key())
        .unwrap();
    let failed_total: Option<i64> = fixture.connection.get(fixture.failed_total_key()).unwrap();
    assert!(processed_total.is_none());
    assert!(failed_total.is_none());
}

#[test]
fn forward_scheduled_moves_due_task_to_pending() {
    let Some(mut fixture) = RedisFixture::new("forward-scheduled") else {
        return;
    };
    let mut client = fixture.client();
    let task = Task::new_with_options(
        "email:welcome",
        b"payload".to_vec(),
        [
            TaskOption::queue(fixture.queue()),
            TaskOption::task_id("task-id"),
            TaskOption::process_in(Duration::from_secs(3600)),
        ],
    );

    client.enqueue(&task).unwrap();
    let not_due = client
        .broker_mut()
        .forward_scheduled(fixture.queue())
        .unwrap();
    assert_eq!(not_due, 0);
    let pending_ids: Vec<String> = fixture
        .connection
        .lrange(fixture.pending_key(), 0, -1)
        .unwrap();
    assert!(pending_ids.is_empty());

    let _: usize = fixture
        .connection
        .zadd(fixture.scheduled_key(), "task-id", 0)
        .unwrap();
    let moved = client
        .broker_mut()
        .forward_scheduled(fixture.queue())
        .unwrap();

    assert_eq!(moved, 1);
    let pending_ids: Vec<String> = fixture
        .connection
        .lrange(fixture.pending_key(), 0, -1)
        .unwrap();
    assert_eq!(pending_ids, ["task-id"]);
    let scheduled_score: Option<f64> = fixture
        .connection
        .zscore(fixture.scheduled_key(), "task-id")
        .unwrap();
    assert!(scheduled_score.is_none());
    let stored: HashMap<String, Vec<u8>> = fixture
        .connection
        .hgetall(fixture.task_key("task-id"))
        .unwrap();
    assert_eq!(string_field(&stored, "state"), "pending");
    assert!(stored.contains_key("pending_since"));
}

#[test]
fn forward_retry_moves_due_task_to_pending() {
    let Some(mut fixture) = RedisFixture::new("forward-retry") else {
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

    client.enqueue(&task).unwrap();
    let dequeued = client
        .broker_mut()
        .dequeue(&[fixture.queue().to_owned()])
        .unwrap();
    client
        .broker_mut()
        .retry(
            dequeued.message(),
            std::time::SystemTime::now() + Duration::from_secs(3600),
            "handler failed",
            true,
        )
        .unwrap();
    let not_due = client.broker_mut().forward_retry(fixture.queue()).unwrap();
    assert_eq!(not_due, 0);

    let _: usize = fixture
        .connection
        .zadd(fixture.retry_key(), "task-id", 0)
        .unwrap();
    let moved = client.broker_mut().forward_retry(fixture.queue()).unwrap();

    assert_eq!(moved, 1);
    let pending_ids: Vec<String> = fixture
        .connection
        .lrange(fixture.pending_key(), 0, -1)
        .unwrap();
    assert_eq!(pending_ids, ["task-id"]);
    let retry_score: Option<f64> = fixture
        .connection
        .zscore(fixture.retry_key(), "task-id")
        .unwrap();
    assert!(retry_score.is_none());
    let stored: HashMap<String, Vec<u8>> = fixture
        .connection
        .hgetall(fixture.task_key("task-id"))
        .unwrap();
    assert_eq!(string_field(&stored, "state"), "pending");
    assert!(stored.contains_key("pending_since"));
}

#[test]
fn archive_moves_active_task_to_archived_set_and_records_failure_stats() {
    let Some(mut fixture) = RedisFixture::new("archive") else {
        return;
    };
    let mut client = fixture.client();
    let task = Task::new_with_options(
        "email:welcome",
        b"payload".to_vec(),
        [
            TaskOption::queue(fixture.queue()),
            TaskOption::task_id("task-id"),
            TaskOption::max_retry(0),
        ],
    );

    client.enqueue(&task).unwrap();
    let dequeued = client
        .broker_mut()
        .dequeue(&[fixture.queue().to_owned()])
        .unwrap();
    client
        .broker_mut()
        .archive(
            dequeued.message(),
            std::time::SystemTime::now(),
            "max retry exhausted",
            true,
        )
        .unwrap();

    let active_ids: Vec<String> = fixture
        .connection
        .lrange(fixture.active_key(), 0, -1)
        .unwrap();
    assert!(active_ids.is_empty());
    let lease_score: Option<f64> = fixture
        .connection
        .zscore(fixture.lease_key(), "task-id")
        .unwrap();
    assert!(lease_score.is_none());
    let archived_score: f64 = fixture
        .connection
        .zscore(fixture.archived_key(), "task-id")
        .unwrap();
    assert!(archived_score > 0.0);

    let stored: HashMap<String, Vec<u8>> = fixture
        .connection
        .hgetall(fixture.task_key("task-id"))
        .unwrap();
    assert_eq!(string_field(&stored, "state"), "archived");
    let archived_msg = decode_msg(stored.get("msg").unwrap());
    assert_eq!(archived_msg.retried, 1);
    assert_eq!(archived_msg.error_msg, "max retry exhausted");
    assert!(archived_msg.last_failed_at > 0);

    let processed_total: i64 = fixture
        .connection
        .get(fixture.processed_total_key())
        .unwrap();
    let failed_total: i64 = fixture.connection.get(fixture.failed_total_key()).unwrap();
    assert_eq!(processed_total, 1);
    assert_eq!(failed_total, 1);
}

#[test]
fn recover_expired_leases_routes_tasks_to_retry_or_archive() {
    let Some(mut fixture) = RedisFixture::new("recover") else {
        return;
    };
    let mut client = fixture.client();
    let retry_task = Task::new_with_options(
        "email:welcome",
        b"retry".to_vec(),
        [
            TaskOption::queue(fixture.queue()),
            TaskOption::task_id("retry-id"),
            TaskOption::max_retry(5),
        ],
    );
    let archive_task = Task::new_with_options(
        "email:welcome",
        b"archive".to_vec(),
        [
            TaskOption::queue(fixture.queue()),
            TaskOption::task_id("archive-id"),
            TaskOption::max_retry(0),
        ],
    );

    client.enqueue(&retry_task).unwrap();
    client.enqueue(&archive_task).unwrap();
    let retry_dequeued = client
        .broker_mut()
        .dequeue(&[fixture.queue().to_owned()])
        .unwrap();
    let archive_dequeued = client
        .broker_mut()
        .dequeue(&[fixture.queue().to_owned()])
        .unwrap();
    let _: usize = fixture
        .connection
        .zadd(fixture.lease_key(), retry_dequeued.message().id.as_str(), 0)
        .unwrap();
    let _: usize = fixture
        .connection
        .zadd(
            fixture.lease_key(),
            archive_dequeued.message().id.as_str(),
            0,
        )
        .unwrap();
    let result = client
        .broker_mut()
        .recover_expired_leases(
            fixture.queue(),
            std::time::SystemTime::now() + Duration::from_secs(60),
            "lease expired",
        )
        .unwrap();

    assert_eq!(result.retried(), 1);
    assert_eq!(result.archived(), 1);
    let retry_fields: HashMap<String, Vec<u8>> = fixture
        .connection
        .hgetall(fixture.task_key("retry-id"))
        .unwrap();
    let retry_msg = decode_msg(retry_fields.get("msg").unwrap());
    assert_eq!(string_field(&retry_fields, "state"), "retry");
    assert_eq!(retry_msg.retried, 1);
    assert_eq!(retry_msg.error_msg, "lease expired");
    assert!(retry_msg.last_failed_at > 0);
    let retry_score: f64 = fixture
        .connection
        .zscore(fixture.retry_key(), "retry-id")
        .unwrap();
    assert!(retry_score > 0.0);
    let archive_fields: HashMap<String, Vec<u8>> = fixture
        .connection
        .hgetall(fixture.task_key("archive-id"))
        .unwrap();
    let archive_msg = decode_msg(archive_fields.get("msg").unwrap());
    assert_eq!(string_field(&archive_fields, "state"), "archived");
    assert_eq!(archive_msg.retried, 1);
    assert_eq!(archive_msg.error_msg, "lease expired");
    assert!(archive_msg.last_failed_at > 0);
    let archived_score: f64 = fixture
        .connection
        .zscore(fixture.archived_key(), "archive-id")
        .unwrap();
    assert!(archived_score > 0.0);
    let pending_ids: Vec<String> = fixture
        .connection
        .lrange(fixture.pending_key(), 0, -1)
        .unwrap();
    assert!(pending_ids.is_empty());
    let active_ids: Vec<String> = fixture
        .connection
        .lrange(fixture.active_key(), 0, -1)
        .unwrap();
    assert!(active_ids.is_empty());
    let retry_lease_score: Option<f64> = fixture
        .connection
        .zscore(fixture.lease_key(), "retry-id")
        .unwrap();
    let archive_lease_score: Option<f64> = fixture
        .connection
        .zscore(fixture.lease_key(), "archive-id")
        .unwrap();
    assert!(retry_lease_score.is_none());
    assert!(archive_lease_score.is_none());
    let processed_total: i64 = fixture
        .connection
        .get(fixture.processed_total_key())
        .unwrap();
    let failed_total: i64 = fixture.connection.get(fixture.failed_total_key()).unwrap();
    assert_eq!(processed_total, 2);
    assert_eq!(failed_total, 2);
}

#[test]
fn extend_lease_updates_existing_active_lease_only() {
    let Some(mut fixture) = RedisFixture::new("extend-lease") else {
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

    client.enqueue(&task).unwrap();
    let dequeued = client
        .broker_mut()
        .dequeue(&[fixture.queue().to_owned()])
        .unwrap();
    let original_score: f64 = fixture
        .connection
        .zscore(fixture.lease_key(), "task-id")
        .unwrap();
    let extension = client
        .broker_mut()
        .extend_lease(fixture.queue(), &dequeued.message().id)
        .unwrap();

    assert!(extension.expires_at() > dequeued.lease_expires_at());
    let extended_score: f64 = fixture
        .connection
        .zscore(fixture.lease_key(), "task-id")
        .unwrap();
    assert!(extended_score >= original_score);
    client.broker_mut().complete(dequeued.message()).unwrap();
    let missing_extension = client
        .broker_mut()
        .extend_lease(fixture.queue(), &dequeued.message().id)
        .unwrap();
    assert!(missing_extension.expires_at() > dequeued.lease_expires_at());
    let lease_score: Option<f64> = fixture
        .connection
        .zscore(fixture.lease_key(), "task-id")
        .unwrap();
    assert!(lease_score.is_none());
}

#[test]
fn processor_run_once_completes_successful_task() {
    let Some(mut fixture) = RedisFixture::new("processor-complete") else {
        return;
    };
    let mut client = fixture.client();
    let task = Task::with_headers_and_options(
        "email:welcome",
        b"payload".to_vec(),
        [("trace-id", "abc")],
        [
            TaskOption::queue(fixture.queue()),
            TaskOption::task_id("task-id"),
        ],
    );

    client.enqueue(&task).unwrap();
    let broker = client.into_broker();
    let mut processor = Processor::with_retry_delay(
        broker,
        |task: &Task| {
            assert_eq!(task.type_name(), "email:welcome");
            assert_eq!(task.payload(), b"payload");
            assert_eq!(task.header("trace-id"), Some("abc"));
            Ok(())
        },
        |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
    );

    let result = processor.run_once(&[fixture.queue().to_owned()]).unwrap();

    assert_eq!(
        result,
        ProcessorRun::Completed {
            task_id: "task-id".to_owned()
        }
    );
    assert!(
        !fixture
            .connection
            .exists::<_, bool>(fixture.task_key("task-id"))
            .unwrap()
    );
    let processed_total: i64 = fixture
        .connection
        .get(fixture.processed_total_key())
        .unwrap();
    assert_eq!(processed_total, 1);
}

#[test]
fn processor_run_once_retries_handler_failure() {
    let Some(mut fixture) = RedisFixture::new("processor-retry") else {
        return;
    };
    let mut client = fixture.client();
    let task = Task::new_with_options(
        "email:welcome",
        b"payload".to_vec(),
        [
            TaskOption::queue(fixture.queue()),
            TaskOption::task_id("task-id"),
            TaskOption::max_retry(3),
        ],
    );

    client.enqueue(&task).unwrap();
    let broker = client.into_broker();
    let mut processor = Processor::with_retry_delay(
        broker,
        |_task: &Task| Err(HandlerError::failed("handler failed")),
        |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
    );

    let result = processor.run_once(&[fixture.queue().to_owned()]).unwrap();

    let retry_at = match result {
        ProcessorRun::Retried { task_id, retry_at } => {
            assert_eq!(task_id, "task-id");
            retry_at
        }
        other => panic!("expected retry result, got {other:?}"),
    };
    let retry_score: f64 = fixture
        .connection
        .zscore(fixture.retry_key(), "task-id")
        .unwrap();
    let expected = retry_at
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as f64;
    assert_eq!(retry_score, expected);
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
fn processor_is_failure_false_retries_without_failed_counters() {
    let Some(mut fixture) = RedisFixture::new("processor-is-failure") else {
        return;
    };
    let mut client = fixture.client();
    let task = Task::new_with_options(
        "email:welcome",
        b"payload".to_vec(),
        [
            TaskOption::queue(fixture.queue()),
            TaskOption::task_id("task-id"),
            TaskOption::max_retry(3),
        ],
    );

    client.enqueue(&task).unwrap();
    let broker = client.into_broker();
    let mut processor = Processor::with_retry_delay(
        broker,
        |_task: &Task| Err(HandlerError::failed("transient")),
        |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
    )
    .with_is_failure(|error: &HandlerError| error.to_string() != "transient")
    .with_error_handler(RecordingErrorHandler::default());

    let result = processor.run_once(&[fixture.queue().to_owned()]).unwrap();

    assert!(matches!(result, ProcessorRun::Retried { .. }));
    assert_eq!(
        processor.error_handler().errors,
        [("email:welcome".to_owned(), "transient".to_owned())]
    );
    let retry_score: f64 = fixture
        .connection
        .zscore(fixture.retry_key(), "task-id")
        .unwrap();
    assert!(retry_score > 0.0);
    let processed_total: i64 = fixture
        .connection
        .get(fixture.processed_total_key())
        .unwrap();
    assert_eq!(processed_total, 1);
    let failed_total: Option<i64> = fixture.connection.get(fixture.failed_total_key()).unwrap();
    assert!(failed_total.is_none());
    let failed_daily_keys: Vec<String> = fixture
        .connection
        .keys(fixture.failed_daily_key_pattern())
        .unwrap();
    assert!(failed_daily_keys.is_empty());
}

#[test]
fn processor_revoke_deletes_retained_task_without_completed_set() {
    let Some(mut fixture) = RedisFixture::new("processor-revoke") else {
        return;
    };
    let mut client = fixture.client();
    let task = Task::new_with_options(
        "email:welcome",
        b"payload".to_vec(),
        [
            TaskOption::queue(fixture.queue()),
            TaskOption::task_id("task-id"),
            TaskOption::retention(Duration::from_secs(300)),
        ],
    );

    client.enqueue(&task).unwrap();
    let broker = client.into_broker();
    let mut processor = Processor::with_retry_delay(
        broker,
        |_task: &Task| Err(HandlerError::revoke_task("revoke")),
        |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
    );

    let result = processor.run_once(&[fixture.queue().to_owned()]).unwrap();

    assert_eq!(
        result,
        ProcessorRun::Revoked {
            task_id: "task-id".to_owned()
        }
    );
    assert!(
        !fixture
            .connection
            .exists::<_, bool>(fixture.task_key("task-id"))
            .unwrap()
    );
    let completed_score: Option<f64> = fixture
        .connection
        .zscore(fixture.completed_key(), "task-id")
        .unwrap();
    assert!(completed_score.is_none());
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
        RedisScript::EnqueueUnique.result_for_code(-1),
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

    fn active_key(&self) -> String {
        format!("asynq:{{{}}}:active", self.queue)
    }

    fn lease_key(&self) -> String {
        format!("asynq:{{{}}}:lease", self.queue)
    }

    fn completed_key(&self) -> String {
        format!("asynq:{{{}}}:completed", self.queue)
    }

    fn archived_key(&self) -> String {
        format!("asynq:{{{}}}:archived", self.queue)
    }

    fn retry_key(&self) -> String {
        format!("asynq:{{{}}}:retry", self.queue)
    }

    fn processed_total_key(&self) -> String {
        format!("asynq:{{{}}}:processed", self.queue)
    }

    fn failed_total_key(&self) -> String {
        format!("asynq:{{{}}}:failed", self.queue)
    }

    fn processed_daily_key_pattern(&self) -> String {
        format!("asynq:{{{}}}:processed:*", self.queue)
    }

    fn failed_daily_key_pattern(&self) -> String {
        format!("asynq:{{{}}}:failed:*", self.queue)
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

#[derive(Debug, Default)]
struct RecordingErrorHandler {
    errors: Vec<(String, String)>,
}

impl ErrorHandler for RecordingErrorHandler {
    fn handle_error(&mut self, task: &Task, error: &HandlerError) {
        self.errors
            .push((task.type_name().to_owned(), error.to_string()));
    }
}
