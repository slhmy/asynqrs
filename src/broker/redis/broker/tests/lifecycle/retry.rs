use super::*;

#[tokio::test]
async fn async_broker_retries_failed_task_with_retry_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let retry_at = now + Duration::from_secs(60);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let executor = FakeExecutor::default();
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    broker
        .retry_with_now(&msg, now, retry_at, "handler failed", true)
        .await
        .unwrap();

    let calls = &broker.executor().calls;
    assert_eq!(calls.len(), 1);
    let ExecutorCall::EvalScriptStatus { script, keys, args } = &calls[0] else {
        panic!("expected retry script call");
    };
    assert_eq!(*script, RedisScript::Retry);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:lease".to_owned(),
            "asynq:{critical}:retry".to_owned(),
            "asynq:{critical}:processed:2023-11-14".to_owned(),
            "asynq:{critical}:failed:2023-11-14".to_owned(),
            "asynq:{critical}:processed".to_owned(),
            "asynq:{critical}:failed".to_owned(),
        ]
    );
    assert_eq!(args[0], RedisArg::String("task-id".to_owned()));
    assert!(matches!(args[1], RedisArg::Bytes(_)));
    assert_eq!(args[2], RedisArg::I64(1_700_000_060));
    assert_eq!(args[4], RedisArg::String("1".to_owned()));
}

#[tokio::test]
async fn retries_failed_task_with_retry_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let retry_at = now + Duration::from_secs(60);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker
        .retry(&msg, retry_at, "handler failed", true)
        .await
        .unwrap();

    let calls = &broker.executor().calls;
    assert_eq!(calls.len(), 1);
    let ExecutorCall::EvalScriptStatus { script, keys, args } = &calls[0] else {
        panic!("expected retry script call");
    };
    assert_eq!(*script, RedisScript::Retry);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:lease".to_owned(),
            "asynq:{critical}:retry".to_owned(),
            "asynq:{critical}:processed:2023-11-14".to_owned(),
            "asynq:{critical}:failed:2023-11-14".to_owned(),
            "asynq:{critical}:processed".to_owned(),
            "asynq:{critical}:failed".to_owned(),
        ]
    );
    assert_eq!(args[0], RedisArg::String("task-id".to_owned()));
    assert!(matches!(args[1], RedisArg::Bytes(_)));
    assert_eq!(args[2], RedisArg::I64(1_700_000_060));
    assert_eq!(args[4], RedisArg::String("1".to_owned()));
}

#[tokio::test]
async fn worker_broker_core_retries_task_without_dyn_broker_boundary() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let retry_at = now + Duration::from_secs(60);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    crate::server::WorkerBrokerCore::retry(&mut broker, &msg, retry_at, "handler failed", true)
        .await
        .unwrap();

    let calls = &broker.executor().calls;
    assert_eq!(calls.len(), 1);
    let ExecutorCall::EvalScriptStatus { script, .. } = &calls[0] else {
        panic!("expected retry script call");
    };
    assert_eq!(*script, RedisScript::Retry);
}

#[tokio::test]
async fn retry_maps_not_found_errors() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let executor = FakeExecutor {
        script_error: Some(RedisExecutorError::new("redis eval error: NOT FOUND")),
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker
        .retry(&msg, now + Duration::from_secs(60), "handler failed", true)
        .await
        .unwrap_err();

    assert_eq!(error, RetryError::NotFound);
}
