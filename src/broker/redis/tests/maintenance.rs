use super::*;

#[test]
fn async_delete_expired_completed_tasks_removes_only_expired_entries() {
    let Some(mut fixture) = RedisFixture::new("async-completed-cleanup") else {
        return;
    };
    tokio::runtime::Runtime::new().unwrap().block_on(
        async_delete_expired_completed_tasks_removes_only_expired_entries_inner(&mut fixture),
    );
}

async fn async_delete_expired_completed_tasks_removes_only_expired_entries_inner(
    fixture: &mut RedisFixture,
) {
    for task_id in ["expired-id", "fresh-id"] {
        let task = Task::new("email:welcome", b"payload".to_vec());
        fixture
            .enqueue_with(
                &task,
                fixture
                    .enqueue_options(task_id)
                    .retain_for(Duration::from_secs(300)),
            )
            .await;
        let mut broker = fixture.async_broker().await;
        let dequeued =
            crate::server::WorkerBrokerCore::dequeue(&mut broker, &[fixture.queue().to_owned()])
                .await
                .unwrap();
        crate::server::WorkerBrokerCore::complete(&mut broker, dequeued.message())
            .await
            .unwrap();
    }

    let _: usize = fixture
        .connection
        .zadd(fixture.completed_key(), "expired-id", 1)
        .unwrap();
    let mut broker = fixture.async_broker().await;
    let deleted = broker
        .delete_expired_completed_tasks(fixture.queue(), 100)
        .await
        .unwrap();

    assert_eq!(deleted, 1);
    let expired_exists: bool = fixture
        .connection
        .exists(fixture.task_key("expired-id"))
        .unwrap();
    assert!(!expired_exists);
    let fresh_exists: bool = fixture
        .connection
        .exists(fixture.task_key("fresh-id"))
        .unwrap();
    assert!(fresh_exists);
    let completed_ids: Vec<String> = fixture
        .connection
        .zrange(fixture.completed_key(), 0, -1)
        .unwrap();
    assert_eq!(completed_ids, ["fresh-id"]);
}

#[test]
fn async_forward_scheduled_drains_ready_batches() {
    let Some(mut fixture) = RedisFixture::new("async-forward-drain") else {
        return;
    };
    tokio::runtime::Runtime::new().unwrap().block_on(
        async_forward_scheduled_drains_ready_batches_inner(&mut fixture),
    );
}

async fn async_forward_scheduled_drains_ready_batches_inner(fixture: &mut RedisFixture) {
    for index in 0..105 {
        let task_id = format!("scheduled-id-{index:03}");
        let task = Task::new("email:scheduled", b"payload".to_vec());
        fixture
            .enqueue_with(
                &task,
                fixture
                    .enqueue_options(&task_id)
                    .process_in(Duration::from_secs(3600)),
            )
            .await;
        let _: usize = fixture
            .connection
            .zadd(fixture.scheduled_key(), &task_id, 1)
            .unwrap();
    }

    let mut broker = fixture.async_broker().await;
    let moved = broker.forward_scheduled(fixture.queue()).await.unwrap();

    assert_eq!(moved, 105);
    let scheduled_count: usize = fixture.connection.zcard(fixture.scheduled_key()).unwrap();
    assert_eq!(scheduled_count, 0);
    let pending_count: usize = fixture.connection.llen(fixture.pending_key()).unwrap();
    assert_eq!(pending_count, 105);
}
