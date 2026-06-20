use super::*;

#[tokio::test]
async fn async_broker_requeues_active_task_with_requeue_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker.requeue_with_now(&msg).await.unwrap();

    let calls = &broker.executor().calls;
    assert_eq!(calls.len(), 1);
    let ExecutorCall::EvalScriptStatus { script, keys, args } = &calls[0] else {
        panic!("expected requeue script call");
    };
    assert_eq!(*script, RedisScript::Requeue);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:lease".to_owned(),
            "asynq:{critical}:pending".to_owned(),
            "asynq:{critical}:t:task-id".to_owned(),
        ]
    );
    assert_eq!(args, &[RedisArg::String("task-id".to_owned())]);
}

#[tokio::test]
async fn async_broker_requeue_maps_not_found_errors() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let executor = FakeExecutor {
        script_error: Some(RedisExecutorError::new("redis eval error: NOT FOUND")),
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker.requeue_with_now(&msg).await.unwrap_err();

    assert_eq!(error, RequeueError::NotFound);
}

#[tokio::test]
async fn requeues_active_task_with_requeue_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker.requeue(&msg).await.unwrap();

    let calls = &broker.executor().calls;
    assert_eq!(calls.len(), 1);
    let ExecutorCall::EvalScriptStatus { script, keys, args } = &calls[0] else {
        panic!("expected requeue script call");
    };
    assert_eq!(*script, RedisScript::Requeue);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:lease".to_owned(),
            "asynq:{critical}:pending".to_owned(),
            "asynq:{critical}:t:task-id".to_owned(),
        ]
    );
    assert_eq!(args, &[RedisArg::String("task-id".to_owned())]);
}

#[tokio::test]
async fn requeue_maps_not_found_errors() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let executor = FakeExecutor {
        script_error: Some(RedisExecutorError::new("redis eval error: NOT FOUND")),
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker.requeue(&msg).await.unwrap_err();

    assert_eq!(error, RequeueError::NotFound);
}
