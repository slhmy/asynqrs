use super::*;

#[tokio::test]
async fn async_broker_lists_scheduler_enqueue_events() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        zrevrange_bytes_results: vec![vec![
            pb::asynq::SchedulerEnqueueEvent {
                task_id: "task-id".to_owned(),
                enqueue_time: Some(prost_types::Timestamp {
                    seconds: 1_700_000_030,
                    nanos: 0,
                }),
            }
            .encode_to_vec(),
        ]],
        ..Default::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let events = broker
        .list_scheduler_enqueue_events("entry-id", Pagination::new(1, 20).unwrap())
        .await
        .unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].task_id(), "task-id");
    assert_eq!(
        events[0].enqueued_at(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_030)
    );
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::ZrevrangeBytes {
            key: "asynq:scheduler_history:entry-id".to_owned(),
            start: 20,
            stop: 39,
        }]
    );
}

#[tokio::test]
async fn async_broker_public_list_scheduler_enqueue_events_alias_uses_list_options() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        zrevrange_bytes_results: vec![Vec::new()],
        ..Default::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let events = broker
        .list_scheduler_enqueue_events_with_options("entry-id", [page_size(20), page(2)])
        .await
        .unwrap();

    assert!(events.is_empty());
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::ZrevrangeBytes {
            key: "asynq:scheduler_history:entry-id".to_owned(),
            start: 20,
            stop: 39,
        }]
    );
}

#[tokio::test]
async fn async_broker_lists_scheduler_enqueue_events_defaults_missing_enqueue_time() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        zrevrange_bytes_results: vec![vec![
            pb::asynq::SchedulerEnqueueEvent {
                task_id: "task-id".to_owned(),
                enqueue_time: None,
            }
            .encode_to_vec(),
        ]],
        ..Default::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let events = broker
        .list_scheduler_enqueue_events("entry-id", Pagination::new(0, 20).unwrap())
        .await
        .unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].task_id(), "task-id");
    assert_eq!(events[0].enqueued_at(), UNIX_EPOCH);
}

#[tokio::test]
async fn async_broker_lists_scheduler_enqueue_events_allows_zero_page_size() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        zrevrange_bytes_results: vec![Vec::new()],
        ..Default::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let events = broker
        .list_scheduler_enqueue_events("entry-id", Pagination::new(3, 0).unwrap())
        .await
        .unwrap();

    assert!(events.is_empty());
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::ZrevrangeBytes {
            key: "asynq:scheduler_history:entry-id".to_owned(),
            start: 0,
            stop: -1,
        }]
    );
}

#[tokio::test]
async fn async_broker_lists_scheduler_enqueue_events_preserves_asynq_page_zero_range() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        zrevrange_bytes_results: vec![Vec::new()],
        ..Default::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let events = broker
        .list_scheduler_enqueue_events("entry-id", Pagination::from_asynq_options(0, 20).unwrap())
        .await
        .unwrap();

    assert!(events.is_empty());
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::ZrevrangeBytes {
            key: "asynq:scheduler_history:entry-id".to_owned(),
            start: -20,
            stop: -1,
        }]
    );
}

#[tokio::test]
async fn async_broker_lists_scheduler_enqueue_events_allows_empty_entry_id() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        zrevrange_bytes_results: vec![Vec::new()],
        ..Default::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let events = broker
        .list_scheduler_enqueue_events("", Pagination::new(0, 20).unwrap())
        .await
        .unwrap();

    assert!(events.is_empty());
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::ZrevrangeBytes {
            key: "asynq:scheduler_history:".to_owned(),
            start: 0,
            stop: 19,
        }]
    );
}

#[tokio::test]
async fn async_broker_lists_scheduler_enqueue_events_maps_decode_errors_to_admin_errors() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        zrevrange_bytes_results: vec![vec![b"\x80".to_vec()]],
        ..Default::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker
        .list_scheduler_enqueue_events("entry-id", Pagination::new(0, 20).unwrap())
        .await
        .unwrap_err();

    assert!(
        error
            .to_string()
            .starts_with("invalid scheduler enqueue event:")
    );
}

#[tokio::test]
async fn async_broker_records_scheduler_enqueue_event() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker
        .record_scheduler_enqueue_event("entry-id", b"event".to_vec(), now)
        .await
        .unwrap();

    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::EvalScriptStatus {
            script: RedisScript::RecordSchedulerEnqueueEvent,
            keys: vec!["asynq:scheduler_history:entry-id".to_owned()],
            args: vec![
                RedisArg::I64(1_700_000_000),
                RedisArg::Bytes(b"event".to_vec()),
                RedisArg::I64(1000),
            ],
        }]
    );
}

#[tokio::test]
async fn async_broker_clears_scheduler_history() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker.clear_scheduler_history("entry-id").await.unwrap();

    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::Del {
            key: "asynq:scheduler_history:entry-id".to_owned(),
        }]
    );
}
