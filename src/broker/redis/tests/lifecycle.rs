use super::*;

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
    let task = Task::new("email:welcome", b"payload".to_vec());

    let result = fixture
        .enqueue_with(
            &task,
            fixture
                .enqueue_options("task-id")
                .retain_for(Duration::from_secs(300)),
        )
        .await;

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
    let dequeued =
        crate::server::WorkerBrokerCore::dequeue(&mut broker, &[fixture.queue().to_owned()])
            .await
            .unwrap();
    assert_eq!(dequeued.message().id, "task-id");
    assert_eq!(dequeued.message().queue, fixture.queue());
    let stored: HashMap<String, Vec<u8>> = fixture.connection.hgetall(&task_key).unwrap();
    assert_eq!(string_field(&stored, "state"), "active");
    let lease_score: f64 = fixture
        .connection
        .zscore(fixture.lease_key(), "task-id")
        .unwrap();
    assert!(lease_score > 0.0);

    crate::server::WorkerBrokerCore::complete(&mut broker, dequeued.message())
        .await
        .unwrap();

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
fn async_client_enqueue_scope_uses_redis_backed_client() {
    let Some(mut fixture) = RedisFixture::new("async-client-enqueue") else {
        return;
    };
    tokio::runtime::Runtime::new().unwrap().block_on(
        async_client_enqueue_scope_uses_redis_backed_client_inner(&mut fixture),
    );
}

async fn async_client_enqueue_scope_uses_redis_backed_client_inner(fixture: &mut RedisFixture) {
    let redis_client = redis::Client::open(fixture.url.as_ref()).unwrap();
    let mut client = RedisBackedClient::from_direct_redis_client(redis_client)
        .await
        .unwrap();
    let scope = ClientEnqueueScope::background();
    let task = Task::new("email:welcome", b"payload".to_vec());

    let result = client
        .enqueue_scoped_with_async(&scope, &task, fixture.enqueue_options("client-task-id"))
        .await
        .unwrap();

    assert_eq!(result.id(), "client-task-id");
    assert_eq!(result.queue(), fixture.queue());
    assert_eq!(result.state(), TaskState::Pending);
    let task_key = fixture.task_key("client-task-id");
    let stored: HashMap<String, Vec<u8>> = fixture.connection.hgetall(&task_key).unwrap();
    assert_eq!(string_field(&stored, "state"), "pending");
    assert_eq!(
        decode_msg(stored.get("msg").unwrap()).r#type,
        "email:welcome"
    );
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
    let task = Task::new("email:welcome", b"payload".to_vec());

    fixture
        .enqueue_with(&task, fixture.enqueue_options("task-id").max_retries(5))
        .await;
    let mut broker = fixture.async_broker().await;
    let dequeued =
        crate::server::WorkerBrokerCore::dequeue(&mut broker, &[fixture.queue().to_owned()])
            .await
            .unwrap();
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
    let processed_keys = fixture.daily_stat_keys("processed");
    let failed_keys = fixture.daily_stat_keys("failed");
    assert_eq!(processed_keys.len(), 1);
    assert_eq!(failed_keys.len(), 1);
    assert_eq!(
        fixture
            .connection
            .get::<_, i64>(&processed_keys[0])
            .unwrap(),
        1
    );
    assert_eq!(
        fixture.connection.get::<_, i64>(&failed_keys[0]).unwrap(),
        1
    );
    assert_eq!(
        fixture
            .connection
            .get::<_, i64>(fixture.processed_total_key())
            .unwrap(),
        1
    );
    assert_eq!(
        fixture
            .connection
            .get::<_, i64>(fixture.failed_total_key())
            .unwrap(),
        1
    );
}

#[test]
fn async_archive_trims_old_archived_tasks_and_records_failure_stats() {
    let Some(mut fixture) = RedisFixture::new("async-archive") else {
        return;
    };
    tokio::runtime::Runtime::new().unwrap().block_on(
        async_archive_trims_old_archived_tasks_and_records_failure_stats_inner(&mut fixture),
    );
}

async fn async_archive_trims_old_archived_tasks_and_records_failure_stats_inner(
    fixture: &mut RedisFixture,
) {
    let task = Task::new("email:welcome", b"payload".to_vec());
    fixture
        .enqueue_with(&task, fixture.enqueue_options("task-id").max_retries(5))
        .await;
    let old_archived_id = "old-archived-id";
    let old_archive_score = 1_692_223_999_i64;
    let _: () = fixture
        .connection
        .hset_multiple(
            fixture.task_key(old_archived_id),
            &[
                ("msg", b"old-message".as_slice()),
                ("state", b"archived".as_slice()),
            ],
        )
        .unwrap();
    let _: usize = fixture
        .connection
        .zadd(fixture.archived_key(), old_archived_id, old_archive_score)
        .unwrap();

    let mut broker = fixture.async_broker().await;
    let dequeued =
        crate::server::WorkerBrokerCore::dequeue(&mut broker, &[fixture.queue().to_owned()])
            .await
            .unwrap();
    broker
        .archive(dequeued.message(), "max retry exhausted")
        .await
        .unwrap();

    let archived_score: f64 = fixture
        .connection
        .zscore(fixture.archived_key(), "task-id")
        .unwrap();
    assert!(archived_score as i64 >= 1_700_000_000);
    let stored: HashMap<String, Vec<u8>> = fixture
        .connection
        .hgetall(fixture.task_key("task-id"))
        .unwrap();
    assert_eq!(string_field(&stored, "state"), "archived");
    let archived_msg = decode_msg(stored.get("msg").unwrap());
    assert_eq!(archived_msg.retried, 0);
    assert_eq!(archived_msg.error_msg, "max retry exhausted");
    assert!(archived_msg.last_failed_at >= 1_700_000_000);

    let old_task_exists: bool = fixture
        .connection
        .exists(fixture.task_key(old_archived_id))
        .unwrap();
    assert!(!old_task_exists);
    let old_score: Option<f64> = fixture
        .connection
        .zscore(fixture.archived_key(), old_archived_id)
        .ok();
    assert_eq!(old_score, None);

    assert_eq!(fixture.daily_stat_keys("processed").len(), 1);
    assert_eq!(fixture.daily_stat_keys("failed").len(), 1);
    assert_eq!(
        fixture
            .connection
            .get::<_, i64>(fixture.processed_total_key())
            .unwrap(),
        1
    );
    assert_eq!(
        fixture
            .connection
            .get::<_, i64>(fixture.failed_total_key())
            .unwrap(),
        1
    );
}
