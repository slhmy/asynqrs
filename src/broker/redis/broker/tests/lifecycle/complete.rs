use super::*;

#[tokio::test]
async fn async_broker_completes_retained_task_with_mark_as_complete_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    msg.retention = 300;
    let executor = FakeExecutor::default();
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    broker.complete_with_now(&msg, now).await.unwrap();

    let ExecutorCall::EvalScriptStatus { script, keys, args } = &broker.executor().calls[0] else {
        panic!("expected script call");
    };
    assert_eq!(*script, RedisScript::MarkAsComplete);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:lease".to_owned(),
            "asynq:{critical}:completed".to_owned(),
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:processed:2023-11-14".to_owned(),
            "asynq:{critical}:processed".to_owned(),
        ]
    );
    assert_eq!(args[0], RedisArg::String("task-id".to_owned()));
    assert_eq!(args[2], RedisArg::I64(1_700_000_300));
    assert!(matches!(args[3], RedisArg::Bytes(_)));
}

#[tokio::test]
async fn async_broker_complete_maps_executor_errors() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    msg.retention = 30;
    let executor = FakeExecutor {
        script_error: Some(RedisExecutorError::new("connection closed")),
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker.complete_with_now(&msg, now).await.unwrap_err();

    assert_eq!(error, CompleteError::Other("connection closed".to_owned()));
}

#[tokio::test]
async fn completes_zero_retention_task_with_done_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    crate::server::WorkerBrokerCore::complete(&mut broker, &msg)
        .await
        .unwrap();

    let calls = &broker.executor().calls;
    assert_eq!(calls.len(), 1);
    let ExecutorCall::EvalScriptStatus { script, keys, args } = &calls[0] else {
        panic!("expected complete script call");
    };
    assert_eq!(*script, RedisScript::Done);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:lease".to_owned(),
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:processed:2023-11-14".to_owned(),
            "asynq:{critical}:processed".to_owned(),
        ]
    );
    assert_eq!(args[0], RedisArg::String("task-id".to_owned()));
}

#[tokio::test]
async fn worker_broker_core_completes_task_without_dyn_broker_boundary() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    crate::server::WorkerBrokerCore::complete(&mut broker, &msg)
        .await
        .unwrap();

    let calls = &broker.executor().calls;
    assert_eq!(calls.len(), 1);
    let ExecutorCall::EvalScriptStatus { script, .. } = &calls[0] else {
        panic!("expected complete script call");
    };
    assert_eq!(*script, RedisScript::Done);
}

#[tokio::test]
async fn completes_retained_task_with_mark_as_complete_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    msg.retention = 300;
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    crate::server::WorkerBrokerCore::complete(&mut broker, &msg)
        .await
        .unwrap();

    let ExecutorCall::EvalScriptStatus { script, keys, args } = &broker.executor().calls[0] else {
        panic!("expected complete script call");
    };
    assert_eq!(*script, RedisScript::MarkAsComplete);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:lease".to_owned(),
            "asynq:{critical}:completed".to_owned(),
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:processed:2023-11-14".to_owned(),
            "asynq:{critical}:processed".to_owned(),
        ]
    );
    assert_eq!(args[2], RedisArg::I64(1_700_000_300));
    assert!(matches!(args[3], RedisArg::Bytes(_)));
}

#[tokio::test]
async fn complete_maps_not_found_errors() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let executor = FakeExecutor {
        script_error: Some(RedisExecutorError::new("redis eval error: NOT FOUND")),
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = crate::server::WorkerBrokerCore::complete(&mut broker, &msg)
        .await
        .unwrap_err();

    assert_eq!(error, CompleteError::NotFound);
}
