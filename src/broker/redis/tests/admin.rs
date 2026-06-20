use super::*;

#[test]
fn async_admin_queue_primitives_pause_unpause_and_delete_empty_queue() {
    let Some(mut fixture) = RedisFixture::new("async-admin-queue") else {
        return;
    };
    tokio::runtime::Runtime::new().unwrap().block_on(
        async_admin_queue_primitives_pause_unpause_and_delete_empty_queue_inner(&mut fixture),
    );
}

async fn async_admin_queue_primitives_pause_unpause_and_delete_empty_queue_inner(
    fixture: &mut RedisFixture,
) {
    let task = Task::new("email:welcome", b"payload".to_vec());
    fixture
        .enqueue_with(&task, fixture.enqueue_options("task-id"))
        .await;

    let mut broker = fixture.async_broker().await;
    let queues = broker.list_queues().await.unwrap();
    assert!(queues.contains(&fixture.queue().to_owned()));

    broker.pause_queue(fixture.queue()).await.unwrap();
    let paused_at: i64 = fixture.connection.get(fixture.paused_key()).unwrap();
    assert!(paused_at > 0);
    let error = broker.pause_queue(fixture.queue()).await.unwrap_err();
    assert_eq!(error, crate::AdminError::QueueAlreadyPaused);
    let dequeue_error =
        crate::server::WorkerBrokerCore::dequeue(&mut broker, &[fixture.queue().to_owned()])
            .await
            .unwrap_err();
    assert_eq!(dequeue_error, DequeueError::NoProcessableTask);

    broker.unpause_queue(fixture.queue()).await.unwrap();
    let paused_exists: bool = fixture.connection.exists(fixture.paused_key()).unwrap();
    assert!(!paused_exists);

    let error = broker.delete_queue(fixture.queue()).await.unwrap_err();
    assert_eq!(error, crate::AdminError::QueueNotEmpty);

    broker.delete_queue_force(fixture.queue()).await.unwrap();

    let task_exists: bool = fixture
        .connection
        .exists(fixture.task_key("task-id"))
        .unwrap();
    assert!(!task_exists);
    let pending_exists: bool = fixture.connection.exists(fixture.pending_key()).unwrap();
    assert!(!pending_exists);

    let queues = broker.list_queues().await.unwrap();
    assert!(!queues.contains(&fixture.queue().to_owned()));
}

#[test]
fn async_admin_force_delete_queue_rejects_active_tasks() {
    let Some(mut fixture) = RedisFixture::new("async-admin-force-delete-active") else {
        return;
    };
    tokio::runtime::Runtime::new().unwrap().block_on(
        async_admin_force_delete_queue_rejects_active_tasks_inner(&mut fixture),
    );
}

async fn async_admin_force_delete_queue_rejects_active_tasks_inner(fixture: &mut RedisFixture) {
    let task = Task::new("email:active", b"payload".to_vec());
    fixture
        .enqueue_with(&task, fixture.enqueue_options("active-id"))
        .await;

    let mut broker = fixture.async_broker().await;
    let dequeued =
        crate::server::WorkerBrokerCore::dequeue(&mut broker, &[fixture.queue().to_owned()])
            .await
            .unwrap();
    assert_eq!(dequeued.message().id, "active-id");

    let error = broker
        .delete_queue_force(fixture.queue())
        .await
        .unwrap_err();
    assert_eq!(error, crate::AdminError::QueueHasActiveTasks);

    let active_ids: Vec<String> = fixture
        .connection
        .lrange(fixture.active_key(), 0, -1)
        .unwrap();
    assert_eq!(active_ids, ["active-id"]);
    let task_exists: bool = fixture
        .connection
        .exists(fixture.task_key("active-id"))
        .unwrap();
    assert!(task_exists);
}

#[test]
fn async_admin_current_queue_stats_counts_states() {
    let Some(mut fixture) = RedisFixture::new("async-admin-stats") else {
        return;
    };
    tokio::runtime::Runtime::new().unwrap().block_on(
        async_admin_current_queue_stats_counts_states_inner(&mut fixture),
    );
}

async fn async_admin_current_queue_stats_counts_states_inner(fixture: &mut RedisFixture) {
    let pending_task = Task::new("email:pending", b"pending".to_vec());
    fixture
        .enqueue_with(
            &pending_task,
            fixture
                .enqueue_options("pending-id")
                .retain_for(Duration::from_secs(300)),
        )
        .await;

    let retry_task = Task::new("email:retry", b"retry".to_vec());
    fixture
        .enqueue_with(&retry_task, fixture.enqueue_options("retry-id"))
        .await;

    let group_task = Task::new("email:group", b"group".to_vec());
    fixture
        .enqueue_with(
            &group_task,
            fixture
                .enqueue_options("group-id")
                .group(crate::GroupName::new("tenant-a").unwrap()),
        )
        .await;

    let mut broker = fixture.async_broker().await;
    let dequeued =
        crate::server::WorkerBrokerCore::dequeue(&mut broker, &[fixture.queue().to_owned()])
            .await
            .unwrap();
    assert_eq!(dequeued.message().id, "pending-id");
    crate::server::WorkerBrokerCore::complete(&mut broker, dequeued.message())
        .await
        .expect("complete active task");

    let dequeued =
        crate::server::WorkerBrokerCore::dequeue(&mut broker, &[fixture.queue().to_owned()])
            .await
            .unwrap();
    assert_eq!(dequeued.message().id, "retry-id");
    broker
        .retry(
            dequeued.message(),
            SystemTime::now() + Duration::from_secs(60),
            "handler failed",
            true,
        )
        .await
        .expect("retry active task");
    broker.pause_queue(fixture.queue()).await.unwrap();

    let stats = broker.current_queue_stats(fixture.queue()).await.unwrap();

    assert_eq!(stats.queue(), fixture.queue());
    assert!(stats.paused());
    assert_eq!(stats.pending(), 0);
    assert_eq!(stats.active(), 0);
    assert_eq!(stats.retry(), 1);
    assert_eq!(stats.completed(), 1);
    assert_eq!(stats.aggregating(), 1);
    assert_eq!(stats.groups(), 1);
    assert_eq!(stats.processed(), 2);
    assert_eq!(stats.failed(), 1);
    assert_eq!(stats.processed_total(), 2);
    assert_eq!(stats.failed_total(), 1);
    assert_eq!(stats.size(), 3);
    assert!(stats.memory_usage() > 0);
    assert_eq!(stats.latency(), Duration::ZERO);

    let missing = broker
        .current_queue_stats("missing-admin-stats-queue")
        .await
        .unwrap_err();
    assert_eq!(missing, AdminError::QueueNotFound);
}

#[test]
fn async_admin_historical_and_group_stats_read_counts() {
    let Some(mut fixture) = RedisFixture::new("async-admin-historical-groups") else {
        return;
    };
    tokio::runtime::Runtime::new().unwrap().block_on(
        async_admin_historical_and_group_stats_read_counts_inner(&mut fixture),
    );
}

async fn async_admin_historical_and_group_stats_read_counts_inner(fixture: &mut RedisFixture) {
    let now = SystemTime::now();
    let yesterday = now - Duration::from_secs(24 * 60 * 60);
    let queue = fixture.queue().to_owned();
    let _: usize = fixture.connection.sadd("asynq:queues", &queue).unwrap();
    let _: () = fixture
        .connection
        .set(fixture.processed_key(now), 5)
        .unwrap();
    let _: () = fixture.connection.set(fixture.failed_key(now), 2).unwrap();
    let _: () = fixture
        .connection
        .set(fixture.processed_key(yesterday), 7)
        .unwrap();
    let _: () = fixture
        .connection
        .set(fixture.failed_key(yesterday), 3)
        .unwrap();

    for task_id in ["group-id-1", "group-id-2"] {
        let task = Task::new("email:group", task_id.as_bytes().to_vec());
        fixture
            .enqueue_with(
                &task,
                EnqueueOptions::new()
                    .queue(crate::QueueName::new(&queue).unwrap())
                    .task_id(crate::TaskId::new(task_id).unwrap())
                    .group(crate::GroupName::new("tenant-a").unwrap()),
            )
            .await;
    }

    let mut broker = fixture.async_broker().await;
    let historical = broker
        .historical_queue_stats_with_now(&queue, now, 2)
        .await
        .unwrap();
    assert_eq!(historical.len(), 2);
    assert_eq!(historical[0].queue(), queue);
    assert_eq!(historical[0].processed(), 5);
    assert_eq!(historical[0].failed(), 2);
    assert_eq!(historical[1].processed(), 7);
    assert_eq!(historical[1].failed(), 3);

    let groups = broker.group_stats(&queue).await.unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].group(), "tenant-a");
    assert_eq!(groups[0].size(), 2);

    let error = broker
        .historical_queue_stats_with_now(&queue, now, 0)
        .await
        .unwrap_err();
    assert_eq!(error, AdminError::NonPositiveDays);
}

#[test]
fn async_admin_lists_tasks_and_reads_task_info() {
    let Some(mut fixture) = RedisFixture::new("async-admin-task-info") else {
        return;
    };
    tokio::runtime::Runtime::new().unwrap().block_on(
        async_admin_lists_tasks_and_reads_task_info_inner(&mut fixture),
    );
}

async fn async_admin_lists_tasks_and_reads_task_info_inner(fixture: &mut RedisFixture) {
    let pending_task = Task::new("email:pending", b"pending".to_vec());
    fixture
        .enqueue_with(
            &pending_task,
            fixture
                .enqueue_options("pending-id")
                .retain_for(Duration::from_secs(300)),
        )
        .await;

    let retry_task = Task::new("email:retry", b"retry".to_vec());
    fixture
        .enqueue_with(&retry_task, fixture.enqueue_options("retry-id"))
        .await;

    let mut broker = fixture.async_broker().await;
    let dequeued =
        crate::server::WorkerBrokerCore::dequeue(&mut broker, &[fixture.queue().to_owned()])
            .await
            .unwrap();
    assert_eq!(dequeued.message().id, "pending-id");
    crate::server::WorkerBrokerCore::complete(&mut broker, dequeued.message())
        .await
        .expect("complete active task");

    let dequeued =
        crate::server::WorkerBrokerCore::dequeue(&mut broker, &[fixture.queue().to_owned()])
            .await
            .unwrap();
    assert_eq!(dequeued.message().id, "retry-id");
    broker
        .retry(
            dequeued.message(),
            SystemTime::now() + Duration::from_secs(60),
            "handler failed",
            true,
        )
        .await
        .expect("retry active task");

    let completed = broker
        .list_completed_tasks(fixture.queue(), Pagination::new(0, 10).unwrap())
        .await
        .unwrap();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].message().id, "pending-id");
    assert_eq!(completed[0].state(), TaskState::Completed);

    let retries = broker
        .list_retry_tasks(fixture.queue(), Pagination::new(0, 10).unwrap())
        .await
        .unwrap();
    assert_eq!(retries.len(), 1);
    assert_eq!(retries[0].message().id, "retry-id");
    assert_eq!(retries[0].state(), TaskState::Retry);

    let info = broker.task_info(fixture.queue(), "retry-id").await.unwrap();
    assert_eq!(info.message().id, "retry-id");
    assert_eq!(info.message().r#type, "email:retry");
    assert_eq!(info.state(), TaskState::Retry);
    assert!(info.next_process_at().is_some());
    assert!(info.result().is_empty());

    let missing = broker
        .task_info(fixture.queue(), "missing-id")
        .await
        .unwrap_err();
    assert_eq!(missing, AdminError::TaskNotFound);
}

#[test]
fn async_admin_updates_scheduled_task_payload() {
    let Some(mut fixture) = RedisFixture::new("async-admin-update-payload") else {
        return;
    };
    tokio::runtime::Runtime::new().unwrap().block_on(
        async_admin_updates_scheduled_task_payload_inner(&mut fixture),
    );
}

async fn async_admin_updates_scheduled_task_payload_inner(fixture: &mut RedisFixture) {
    let scheduled_task = Task::new("email:scheduled", b"old".to_vec());
    fixture
        .enqueue_with(
            &scheduled_task,
            fixture
                .enqueue_options("scheduled-id")
                .process_in(Duration::from_secs(3600)),
        )
        .await;

    let mut broker = fixture.async_broker().await;
    broker
        .update_task_payload(fixture.queue(), "scheduled-id", b"updated".to_vec())
        .await
        .unwrap();

    let stored: HashMap<String, Vec<u8>> = fixture
        .connection
        .hgetall(fixture.task_key("scheduled-id"))
        .unwrap();
    assert_eq!(string_field(&stored, "state"), "scheduled");
    let message = decode_msg(stored.get("msg").unwrap());
    assert_eq!(message.r#type, "email:scheduled");
    assert_eq!(message.payload, b"updated");
    let scheduled_ids: Vec<String> = fixture
        .connection
        .zrange(fixture.scheduled_key(), 0, -1)
        .unwrap();
    assert_eq!(scheduled_ids, ["scheduled-id"]);

    let pending_task = Task::new("email:pending", b"old".to_vec());
    fixture
        .enqueue_with(&pending_task, fixture.enqueue_options("pending-id"))
        .await;
    let error = broker
        .update_task_payload(fixture.queue(), "pending-id", b"updated".to_vec())
        .await
        .unwrap_err();
    assert_eq!(error, AdminError::TaskNotScheduled);
}
