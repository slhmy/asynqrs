use super::*;

#[tokio::test]
async fn async_broker_archives_failed_task_with_archive_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let executor = FakeExecutor::default();
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    broker
        .archive_with_now(&msg, now, "max retry exhausted")
        .await
        .unwrap();

    let calls = &broker.executor().calls;
    assert_eq!(calls.len(), 1);
    let ExecutorCall::EvalScriptStatus { script, keys, args } = &calls[0] else {
        panic!("expected archive script call");
    };
    assert_eq!(*script, RedisScript::Archive);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:lease".to_owned(),
            "asynq:{critical}:archived".to_owned(),
            "asynq:{critical}:processed:2023-11-14".to_owned(),
            "asynq:{critical}:failed:2023-11-14".to_owned(),
            "asynq:{critical}:processed".to_owned(),
            "asynq:{critical}:failed".to_owned(),
            "asynq:{critical}:t:".to_owned(),
        ]
    );
    assert_eq!(args[0], RedisArg::String("task-id".to_owned()));
    assert!(matches!(args[1], RedisArg::Bytes(_)));
    assert_eq!(args[2], RedisArg::I64(1_700_000_000));
    assert_eq!(args[3], RedisArg::I64(1_692_224_000));
    assert_eq!(args[4], RedisArg::I64(10_000));
}

#[tokio::test]
async fn archives_failed_task_with_archive_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker.archive(&msg, "max retry exhausted").await.unwrap();

    let calls = &broker.executor().calls;
    assert_eq!(calls.len(), 1);
    let ExecutorCall::EvalScriptStatus { script, keys, args } = &calls[0] else {
        panic!("expected archive script call");
    };
    assert_eq!(*script, RedisScript::Archive);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:lease".to_owned(),
            "asynq:{critical}:archived".to_owned(),
            "asynq:{critical}:processed:2023-11-14".to_owned(),
            "asynq:{critical}:failed:2023-11-14".to_owned(),
            "asynq:{critical}:processed".to_owned(),
            "asynq:{critical}:failed".to_owned(),
            "asynq:{critical}:t:".to_owned(),
        ]
    );
    assert_eq!(args[0], RedisArg::String("task-id".to_owned()));
    assert!(matches!(args[1], RedisArg::Bytes(_)));
    assert_eq!(args[2], RedisArg::I64(1_700_000_000));
    assert_eq!(args[3], RedisArg::I64(1_692_224_000));
    assert_eq!(args[4], RedisArg::I64(10_000));
}

#[tokio::test]
async fn worker_broker_core_archives_task_without_dyn_broker_boundary() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    crate::server::WorkerBrokerCore::archive(&mut broker, &msg, "max retry exhausted")
        .await
        .unwrap();

    let calls = &broker.executor().calls;
    assert_eq!(calls.len(), 1);
    let ExecutorCall::EvalScriptStatus { script, .. } = &calls[0] else {
        panic!("expected archive script call");
    };
    assert_eq!(*script, RedisScript::Archive);
}

#[tokio::test]
async fn archive_maps_not_found_errors() {
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
        .archive(&msg, "max retry exhausted")
        .await
        .unwrap_err();

    assert_eq!(error, ArchiveError::NotFound);
}
