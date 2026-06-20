use super::*;

#[tokio::test]
async fn async_broker_trait_extends_existing_leases() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    let extension = LeaseBroker::extend_leases(
        &mut broker,
        "critical",
        &["task-a".to_owned(), "task-b".to_owned()],
    )
    .await
    .unwrap();

    assert_eq!(
        extension.expires_at(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_030)
    );
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::ZaddExistingMany {
            key: "asynq:{critical}:lease".to_owned(),
            score: 1_700_000_030,
            members: vec!["task-a".to_owned(), "task-b".to_owned()],
        }]
    );
}

#[tokio::test]
async fn lists_expired_leases() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut message = TaskMessage::from_task(&Task::new("email:welcome", b"retry".to_vec()));
    message.id = "retry-id".to_owned();
    message.queue = "critical".to_owned();
    message.retry = 3;
    message.retried = 1;
    let executor = FakeExecutor {
        script_byte_vec_results: vec![vec![message.encode_to_vec()]],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let messages = broker
        .list_lease_expired_with_now(&["critical".to_owned()], now)
        .await
        .unwrap();

    assert_eq!(messages, [message]);
    assert_eq!(broker.executor().calls.len(), 1);
    let ExecutorCall::EvalScriptByteVec { script, keys, args } = &broker.executor().calls[0] else {
        panic!("expected list expired lease script call");
    };
    assert_eq!(*script, RedisScript::ListLeaseExpired);
    assert_eq!(keys, &["asynq:{critical}:lease".to_owned()]);
    assert_eq!(
        args,
        &[
            RedisArg::I64(1_699_999_970),
            RedisArg::String("asynq:{critical}:t:".to_owned()),
        ]
    );
}

#[tokio::test]
async fn extends_existing_lease() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    let extension = broker
        .extend_lease_with_now("critical", "task-id", now)
        .await
        .unwrap();

    assert_eq!(
        extension.expires_at(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_030)
    );
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::ZaddExistingMany {
            key: "asynq:{critical}:lease".to_owned(),
            score: 1_700_000_030,
            members: vec!["task-id".to_owned()],
        }]
    );
}

#[tokio::test]
async fn reports_missing_lease_without_creating_one() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        zadd_existing_results: vec![0],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let extension = broker
        .extend_lease_with_now("critical", "task-id", now)
        .await
        .unwrap();

    assert_eq!(
        extension.expires_at(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_030)
    );
}
