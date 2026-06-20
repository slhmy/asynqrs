use super::*;

#[tokio::test]
async fn async_broker_lists_expired_leases() {
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
async fn async_broker_list_expired_leases_allows_blank_queue_name() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_byte_vec_results: vec![Vec::new()],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let messages = broker
        .list_lease_expired_with_now(&[" ".to_owned()], now)
        .await
        .unwrap();

    assert!(messages.is_empty());
    assert_eq!(broker.executor().calls.len(), 1);
    let ExecutorCall::EvalScriptByteVec { script, keys, args } = &broker.executor().calls[0] else {
        panic!("expected list expired lease script call");
    };
    assert_eq!(*script, RedisScript::ListLeaseExpired);
    assert_eq!(keys, &["asynq:{ }:lease".to_owned()]);
    assert_eq!(
        args,
        &[
            RedisArg::I64(1_699_999_970),
            RedisArg::String("asynq:{ }:t:".to_owned()),
        ]
    );
}

#[tokio::test]
async fn async_broker_lists_expired_leases_across_queues() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut critical = TaskMessage::from_task(&Task::new("email:welcome", b"retry".to_vec()));
    critical.id = "critical-id".to_owned();
    critical.queue = "critical".to_owned();
    let mut default = TaskMessage::from_task(&Task::new("email:welcome", b"retry".to_vec()));
    default.id = "default-id".to_owned();
    default.queue = "default".to_owned();
    let executor = FakeExecutor {
        script_byte_vec_results: vec![
            vec![critical.encode_to_vec()],
            vec![default.encode_to_vec()],
        ],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let messages = broker
        .list_lease_expired_with_now(&["critical".to_owned(), "default".to_owned()], now)
        .await
        .unwrap();

    assert_eq!(messages, [critical, default]);
    assert_eq!(broker.executor().calls.len(), 2);
    let ExecutorCall::EvalScriptByteVec {
        script,
        keys,
        args: _,
    } = &broker.executor().calls[0]
    else {
        panic!("expected list expired lease script call");
    };
    assert_eq!(*script, RedisScript::ListLeaseExpired);
    assert_eq!(keys, &["asynq:{critical}:lease".to_owned()]);
    let ExecutorCall::EvalScriptByteVec {
        script,
        keys,
        args: _,
    } = &broker.executor().calls[1]
    else {
        panic!("expected list expired lease script call");
    };
    assert_eq!(*script, RedisScript::ListLeaseExpired);
    assert_eq!(keys, &["asynq:{default}:lease".to_owned()]);
}
