use super::*;

#[test]
fn async_aggregation_primitives_round_trip_group_tasks() {
    let Some(mut fixture) = RedisFixture::new("async-aggregation") else {
        return;
    };
    tokio::runtime::Runtime::new().unwrap().block_on(
        async_aggregation_primitives_round_trip_group_tasks_inner(&mut fixture),
    );
}

async fn async_aggregation_primitives_round_trip_group_tasks_inner(fixture: &mut RedisFixture) {
    for task_id in ["group-id-1", "group-id-2"] {
        let task = Task::new("email:welcome", task_id.as_bytes().to_vec());
        fixture
            .enqueue_with(
                &task,
                fixture
                    .enqueue_options(task_id)
                    .group(crate::GroupName::new("tenant-a").unwrap()),
            )
            .await;
    }

    let group_ids: Vec<String> = fixture
        .connection
        .zrange(fixture.group_key("tenant-a"), 0, -1)
        .unwrap();
    assert_eq!(group_ids, ["group-id-1", "group-id-2"]);

    let mut broker = fixture.async_broker().await;
    let set_id = broker
        .aggregation_check(
            fixture.queue(),
            "tenant-a",
            Duration::from_secs(60),
            Duration::from_secs(0),
            2,
        )
        .await
        .unwrap();
    let set_id = set_id.unwrap();

    let set_key = fixture.aggregation_set_key("tenant-a", &set_id);
    let set_ids: Vec<String> = fixture.connection.zrange(&set_key, 0, -1).unwrap();
    assert_eq!(set_ids, ["group-id-1", "group-id-2"]);
    let group_ids: Vec<String> = fixture
        .connection
        .zrange(fixture.group_key("tenant-a"), 0, -1)
        .unwrap();
    assert!(group_ids.is_empty());

    let set = broker
        .read_aggregation_set(fixture.queue(), "tenant-a", &set_id)
        .await
        .unwrap();
    let ids: Vec<&str> = set
        .messages
        .iter()
        .map(|message| message.id.as_str())
        .collect();
    assert_eq!(ids, ["group-id-1", "group-id-2"]);
    assert!(set.deadline() > SystemTime::now());

    broker
        .delete_aggregation_set(fixture.queue(), "tenant-a", &set_id)
        .await
        .unwrap();

    let set_exists: bool = fixture.connection.exists(&set_key).unwrap();
    assert!(!set_exists);
    for task_id in ["group-id-1", "group-id-2"] {
        let task_exists: bool = fixture
            .connection
            .exists(fixture.task_key(task_id))
            .unwrap();
        assert!(!task_exists);
    }
}

#[test]
fn async_reclaim_stale_aggregation_sets_moves_tasks_back_to_group() {
    let Some(mut fixture) = RedisFixture::new("async-aggregation-reclaim") else {
        return;
    };
    tokio::runtime::Runtime::new().unwrap().block_on(
        async_reclaim_stale_aggregation_sets_moves_tasks_back_to_group_inner(&mut fixture),
    );
}

async fn async_reclaim_stale_aggregation_sets_moves_tasks_back_to_group_inner(
    fixture: &mut RedisFixture,
) {
    let task = Task::new("email:welcome", b"payload".to_vec());
    fixture
        .enqueue_with(
            &task,
            fixture
                .enqueue_options("group-id")
                .group(crate::GroupName::new("tenant-a").unwrap()),
        )
        .await;

    let mut broker = fixture.async_broker().await;
    let set_id = broker
        .aggregation_check(
            fixture.queue(),
            "tenant-a",
            Duration::from_secs(60),
            Duration::from_secs(0),
            1,
        )
        .await
        .unwrap();
    let set_id = set_id.unwrap();

    let set_key = fixture.aggregation_set_key("tenant-a", &set_id);
    let _: usize = fixture
        .connection
        .zadd(fixture.all_aggregation_sets_key(), &set_key, 1)
        .unwrap();
    broker
        .reclaim_stale_aggregation_sets(fixture.queue())
        .await
        .unwrap();

    let set_exists: bool = fixture.connection.exists(&set_key).unwrap();
    assert!(!set_exists);
    let group_ids: Vec<String> = fixture
        .connection
        .zrange(fixture.group_key("tenant-a"), 0, -1)
        .unwrap();
    assert_eq!(group_ids, ["group-id"]);
}
