use super::*;

#[tokio::test]
async fn async_broker_reads_retry_task_info() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut message = TaskMessage::from_task(&Task::new("email:retry", b"payload".to_vec()));
    message.id = "retry-id".to_owned();
    message.queue = "critical".to_owned();
    let executor = FakeExecutor {
        script_value_results: vec![task_info_value(
            &message,
            "retry",
            1_700_000_120,
            b"result-data".to_vec(),
        )],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let info = broker.task_info("critical", "retry-id").await.unwrap();

    assert_eq!(info.message().id, "retry-id");
    assert_eq!(info.state(), TaskState::Retry);
    assert_eq!(
        info.next_process_at(),
        Some(UNIX_EPOCH + Duration::from_secs(1_700_000_120))
    );
    assert_eq!(info.result(), b"result-data");
    assert!(matches!(
        &broker.executor().calls[0],
        ExecutorCall::Sismember { key, member }
            if key == "asynq:queues" && member == "critical"
    ));
    assert!(matches!(
        &broker.executor().calls[1],
        ExecutorCall::EvalScriptValue { script, keys, args }
            if *script == RedisScript::TaskInfo
                && keys == &["asynq:{critical}:t:retry-id".to_owned()]
                && args == &[
                    RedisArg::String("retry-id".to_owned()),
                    RedisArg::I64(1_700_000_000),
                    RedisArg::String("asynq:{critical}:".to_owned()),
                ]
    ));
}

#[tokio::test]
async fn async_broker_task_info_allows_blank_queue_name() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut message = TaskMessage::from_task(&Task::new("email:retry", b"payload".to_vec()));
    message.id = "retry-id".to_owned();
    message.queue = " ".to_owned();
    let executor = FakeExecutor {
        script_value_results: vec![task_info_value(
            &message,
            "retry",
            1_700_000_120,
            Vec::new(),
        )],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let info = broker.task_info(" ", "retry-id").await.unwrap();

    assert_eq!(info.message().queue, " ");
    assert_eq!(info.state(), TaskState::Retry);
    assert!(matches!(
        &broker.executor().calls[0],
        ExecutorCall::Sismember { key, member }
            if key == "asynq:queues" && member == " "
    ));
    assert!(matches!(
        &broker.executor().calls[1],
        ExecutorCall::EvalScriptValue { script, keys, args }
            if *script == RedisScript::TaskInfo
                && keys == &["asynq:{ }:t:retry-id".to_owned()]
                && args == &[
                    RedisArg::String("retry-id".to_owned()),
                    RedisArg::I64(1_700_000_000),
                    RedisArg::String("asynq:{ }:".to_owned()),
                ]
    ));
}

#[tokio::test]
async fn async_broker_reads_scheduled_task_info_next_process_at_from_zset() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut message = TaskMessage::from_task(&Task::new("email:scheduled", b"payload".to_vec()));
    message.id = "scheduled-id".to_owned();
    message.queue = "critical".to_owned();
    let executor = FakeExecutor {
        script_value_results: vec![task_info_value(
            &message,
            "scheduled",
            1_700_000_180,
            Vec::new(),
        )],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let info = broker.task_info("critical", "scheduled-id").await.unwrap();

    assert_eq!(info.state(), TaskState::Scheduled);
    assert_eq!(
        info.next_process_at(),
        Some(UNIX_EPOCH + Duration::from_secs(1_700_000_180))
    );
    assert!(
        matches!(broker.executor().calls.last(), Some(ExecutorCall::EvalScriptValue { script, .. }) if *script == RedisScript::TaskInfo)
    );
}

#[tokio::test]
async fn async_broker_reads_pending_task_info_next_process_at_from_clock() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut message = TaskMessage::from_task(&Task::new("email:pending", b"payload".to_vec()));
    message.id = "pending-id".to_owned();
    message.queue = "critical".to_owned();
    let executor = FakeExecutor {
        script_value_results: vec![task_info_value(
            &message,
            "pending",
            1_700_000_000,
            Vec::new(),
        )],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let info = broker.task_info("critical", "pending-id").await.unwrap();

    assert_eq!(info.state(), TaskState::Pending);
    assert_eq!(info.next_process_at(), Some(now));
    assert!(
        matches!(broker.executor().calls.last(), Some(ExecutorCall::EvalScriptValue { script, .. }) if *script == RedisScript::TaskInfo)
    );
}

#[tokio::test]
async fn async_broker_task_info_reads_clock_after_queue_check() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let call_log = Arc::new(Mutex::new(Vec::new()));
    let mut message = TaskMessage::from_task(&Task::new("email:pending", b"payload".to_vec()));
    message.id = "pending-id".to_owned();
    message.queue = "critical".to_owned();
    let executor = FakeExecutor {
        call_log: Some(Arc::clone(&call_log)),
        script_value_results: vec![task_info_value(
            &message,
            "pending",
            1_700_000_000,
            Vec::new(),
        )],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(
        executor,
        RecordingClock {
            now,
            call_log: Arc::clone(&call_log),
        },
    );

    broker.task_info("critical", "pending-id").await.unwrap();

    assert_eq!(&*call_log.lock().unwrap(), &["sismember", "clock"]);
    let ExecutorCall::EvalScriptValue { args, .. } = &broker.executor().calls[1] else {
        panic!("expected task info script call");
    };
    assert_eq!(args[1], RedisArg::I64(1_700_000_000));
}

#[tokio::test]
async fn async_broker_reads_non_scheduled_task_info_with_no_next_process_at() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    for state in [
        TaskState::Active,
        TaskState::Archived,
        TaskState::Completed,
        TaskState::Aggregating,
    ] {
        let task_id = format!("{}-id", state.as_str());
        let mut message = TaskMessage::from_task(&Task::new(
            format!("email:{}", state.as_str()),
            b"payload".to_vec(),
        ));
        message.id = task_id.clone();
        message.queue = "critical".to_owned();
        let executor = FakeExecutor {
            script_value_results: vec![task_info_value(&message, state.as_str(), 0, Vec::new())],
            ..FakeExecutor::default()
        };
        let mut broker = RedisBroker::with_clock(executor, TestClock(now));

        let info = broker.task_info("critical", &task_id).await.unwrap();

        assert_eq!(info.state(), state);
        assert_eq!(info.next_process_at(), None);
        assert!(
            matches!(broker.executor().calls.last(), Some(ExecutorCall::EvalScriptValue { script, .. }) if *script == RedisScript::TaskInfo)
        );
    }
}

#[tokio::test]
async fn async_broker_task_info_parses_state_with_upstream_helper() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut message = TaskMessage::from_task(&Task::new("email:unknown", b"payload".to_vec()));
    message.id = "unknown-id".to_owned();
    message.queue = "critical".to_owned();
    let executor = FakeExecutor {
        script_value_results: vec![task_info_value(&message, "unknown", 0, Vec::new())],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker
        .task_info("critical", "unknown-id")
        .await
        .unwrap_err();

    assert_eq!(
        error,
        AdminError::Other("\"unknown\" is not supported task state".to_owned())
    );
}

#[tokio::test]
async fn async_broker_task_info_reports_missing_task() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_error: Some(RedisExecutorError::new("NOT FOUND")),
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker
        .task_info("critical", "missing-id")
        .await
        .unwrap_err();

    assert_eq!(error, AdminError::TaskNotFound);
}
