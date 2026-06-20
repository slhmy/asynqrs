use super::*;

#[tokio::test]
async fn async_broker_dequeues_first_available_task() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let executor = FakeExecutor {
        script_bytes_results: vec![None, Some(msg.encode_to_vec())],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let task = broker
        .dequeue_with_now(&["empty".to_owned(), "critical".to_owned()], now)
        .await
        .unwrap();

    assert_eq!(task.message(), &msg);
    assert_eq!(
        task.lease_expires_at(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_030)
    );
    assert_eq!(broker.executor().calls.len(), 2);
    let ExecutorCall::EvalScriptBytes { script, keys, args } = &broker.executor().calls[0] else {
        panic!("expected script call");
    };
    assert_eq!(*script, RedisScript::Dequeue);
    assert_eq!(
        keys,
        &[
            "asynq:{empty}:pending".to_owned(),
            "asynq:{empty}:paused".to_owned(),
            "asynq:{empty}:active".to_owned(),
            "asynq:{empty}:lease".to_owned(),
        ]
    );
    assert_eq!(
        args,
        &[
            RedisArg::I64(1_700_000_030),
            RedisArg::String("asynq:{empty}:t:".to_owned()),
        ]
    );
}

#[tokio::test]
async fn async_broker_dequeue_allows_blank_queue_name() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_bytes_results: vec![None],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker
        .dequeue_with_now(&[" ".to_owned()], now)
        .await
        .unwrap_err();

    assert_eq!(error, DequeueError::NoProcessableTask);
    assert_eq!(broker.executor().calls.len(), 1);
    let ExecutorCall::EvalScriptBytes { script, keys, args } = &broker.executor().calls[0] else {
        panic!("expected script call");
    };
    assert_eq!(*script, RedisScript::Dequeue);
    assert_eq!(
        keys,
        &[
            "asynq:{ }:pending".to_owned(),
            "asynq:{ }:paused".to_owned(),
            "asynq:{ }:active".to_owned(),
            "asynq:{ }:lease".to_owned(),
        ]
    );
    assert_eq!(
        args,
        &[
            RedisArg::I64(1_700_000_030),
            RedisArg::String("asynq:{ }:t:".to_owned()),
        ]
    );
}

#[tokio::test]
async fn async_broker_dequeue_empty_queue_list_returns_no_processable_task() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    let error = broker.dequeue_with_now(&[], now).await.unwrap_err();

    assert_eq!(error, DequeueError::NoProcessableTask);
    assert!(broker.executor().calls.is_empty());
}

#[tokio::test]
async fn worker_broker_core_dequeues_task_without_dyn_broker_boundary() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let executor = FakeExecutor {
        script_bytes_results: vec![Some(msg.encode_to_vec())],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let dequeued = crate::server::WorkerBrokerCore::dequeue(&mut broker, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(dequeued.message().id, "task-id");
}

#[tokio::test]
async fn worker_broker_core_uses_fresh_lease_clock_per_queue_like_upstream() {
    let empty_now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let critical_now = UNIX_EPOCH + Duration::from_secs(1_700_000_001);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let executor = FakeExecutor {
        script_bytes_results: vec![None, Some(msg.encode_to_vec())],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(
        executor,
        SequenceRecordingClock {
            times: Arc::new(Mutex::new(vec![critical_now, empty_now])),
            call_log: Arc::new(Mutex::new(Vec::new())),
        },
    );

    let dequeued = crate::server::WorkerBrokerCore::dequeue(
        &mut broker,
        &["empty".to_owned(), "critical".to_owned()],
    )
    .await
    .unwrap();

    assert_eq!(dequeued.message().id, "task-id");
    assert_eq!(
        dequeued.lease_expires_at(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_031)
    );
    let calls = &broker.executor().calls;
    let ExecutorCall::EvalScriptBytes { args, .. } = &calls[0] else {
        panic!("expected first dequeue script call");
    };
    assert_eq!(args[0], RedisArg::I64(1_700_000_030));
    let ExecutorCall::EvalScriptBytes { args, .. } = &calls[1] else {
        panic!("expected second dequeue script call");
    };
    assert_eq!(args[0], RedisArg::I64(1_700_000_031));
}

#[tokio::test]
async fn dequeues_first_available_task() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let executor = FakeExecutor {
        script_bytes_results: vec![None, Some(msg.encode_to_vec())],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let task = crate::server::WorkerBrokerCore::dequeue(
        &mut broker,
        &["empty".to_owned(), "critical".to_owned()],
    )
    .await
    .unwrap();

    assert_eq!(task.message(), &msg);
    assert_eq!(
        task.lease_expires_at(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_030)
    );
    assert_eq!(broker.executor().calls.len(), 2);
    let ExecutorCall::EvalScriptBytes { script, keys, args } = &broker.executor().calls[0] else {
        panic!("expected dequeue script call");
    };
    assert_eq!(*script, RedisScript::Dequeue);
    assert_eq!(
        keys,
        &[
            "asynq:{empty}:pending".to_owned(),
            "asynq:{empty}:paused".to_owned(),
            "asynq:{empty}:active".to_owned(),
            "asynq:{empty}:lease".to_owned(),
        ]
    );
    assert_eq!(
        args,
        &[
            RedisArg::I64(1_700_000_030),
            RedisArg::String("asynq:{empty}:t:".to_owned()),
        ]
    );
}

#[tokio::test]
async fn dequeue_reports_no_processable_task() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_bytes_results: vec![None],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = crate::server::WorkerBrokerCore::dequeue(&mut broker, &["critical".to_owned()])
        .await
        .unwrap_err();

    assert_eq!(error, DequeueError::NoProcessableTask);
}

#[tokio::test]
async fn dequeue_maps_executor_errors() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_error: Some(RedisExecutorError::new("connection closed")),
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = crate::server::WorkerBrokerCore::dequeue(&mut broker, &["critical".to_owned()])
        .await
        .unwrap_err();

    assert_eq!(error, DequeueError::Other("connection closed".to_owned()));
}
