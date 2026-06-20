use super::*;

#[tokio::test]
async fn async_broker_lists_pending_tasks() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut first = TaskMessage::from_task(&Task::new("email:first", b"first".to_vec()));
    first.id = "first-id".to_owned();
    first.queue = "critical".to_owned();
    let mut second = TaskMessage::from_task(&Task::new("email:second", b"second".to_vec()));
    second.id = "second-id".to_owned();
    second.queue = "critical".to_owned();
    let executor = FakeExecutor {
        script_byte_vec_results: vec![vec![
            second.encode_to_vec(),
            Vec::new(),
            Vec::new(),
            first.encode_to_vec(),
            Vec::new(),
            b"first-result".to_vec(),
        ]],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));
    let pagination = Pagination::new(1, 2).unwrap();

    let tasks = broker
        .list_pending_tasks("critical", pagination)
        .await
        .unwrap();

    assert_eq!(tasks.len(), 2);
    assert_eq!(tasks[0].message().id, "first-id");
    assert_eq!(tasks[1].message().id, "second-id");
    assert_eq!(tasks[0].state(), TaskState::Pending);
    assert_eq!(tasks[0].next_process_at(), Some(now));
    assert_eq!(tasks[0].result(), b"first-result");
    assert!(matches!(
        &broker.executor().calls[0],
        ExecutorCall::Sismember { key, member }
            if key == "asynq:queues" && member == "critical"
    ));
    assert!(matches!(
        &broker.executor().calls[1],
        ExecutorCall::EvalScriptByteVec { script, keys, args }
            if *script == RedisScript::ListTasks
                && keys == &["asynq:{critical}:pending".to_owned()]
                && args == &[
                    RedisArg::I64(-4),
                    RedisArg::I64(-3),
                    RedisArg::String("asynq:{critical}:t:".to_owned()),
                    RedisArg::String("1".to_owned()),
                ]
    ));
}

#[tokio::test]
async fn async_broker_public_list_pending_tasks_alias_uses_list_options() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut message = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    message.id = "task-id".to_owned();
    message.queue = "critical".to_owned();
    let executor = FakeExecutor {
        script_byte_vec_results: vec![vec![message.encode_to_vec(), Vec::new(), Vec::new()]],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let tasks = broker
        .list_pending_tasks_with_options("critical", [page_size(2), page(2)])
        .await
        .unwrap();

    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].id(), "task-id");
    assert!(matches!(
        &broker.executor().calls[1],
        ExecutorCall::EvalScriptByteVec { script, keys, args }
            if *script == RedisScript::ListTasks
                && keys == &["asynq:{critical}:pending".to_owned()]
                && args == &[
                    RedisArg::I64(-4),
                    RedisArg::I64(-3),
                    RedisArg::String("asynq:{critical}:t:".to_owned()),
                    RedisArg::String("1".to_owned()),
                ]
    ));
}

#[tokio::test]
async fn async_broker_list_tasks_skips_malformed_messages() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut message = TaskMessage::from_task(&Task::new("email:valid", b"payload".to_vec()));
    message.id = "valid-id".to_owned();
    message.queue = "critical".to_owned();
    let executor = FakeExecutor {
        script_byte_vec_results: vec![vec![
            b"malformed".to_vec(),
            b"1700000001".to_vec(),
            b"bad-result".to_vec(),
            message.encode_to_vec(),
            b"1700000120".to_vec(),
            b"valid-result".to_vec(),
        ]],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));
    let pagination = Pagination::new(0, 10).unwrap();

    let tasks = broker
        .list_retry_tasks("critical", pagination)
        .await
        .unwrap();

    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].id(), "valid-id");
    assert_eq!(tasks[0].state(), TaskState::Retry);
    assert_eq!(
        tasks[0].next_process_at(),
        Some(UNIX_EPOCH + Duration::from_secs(1_700_000_120))
    );
    assert_eq!(tasks[0].result(), b"valid-result");
}

#[tokio::test]
async fn async_broker_list_zset_tasks_rejects_malformed_scores() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut bad_score = TaskMessage::from_task(&Task::new("email:bad-score", b"payload".to_vec()));
    bad_score.id = "bad-score-id".to_owned();
    bad_score.queue = "critical".to_owned();
    let mut valid = TaskMessage::from_task(&Task::new("email:valid", b"payload".to_vec()));
    valid.id = "valid-id".to_owned();
    valid.queue = "critical".to_owned();
    let executor = FakeExecutor {
        script_byte_vec_results: vec![vec![
            bad_score.encode_to_vec(),
            b"not-a-score".to_vec(),
            b"bad-result".to_vec(),
            valid.encode_to_vec(),
            b"1700000120".to_vec(),
            b"valid-result".to_vec(),
        ]],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));
    let pagination = Pagination::new(0, 10).unwrap();

    let tasks = broker.list_scheduled_tasks("critical", pagination).await;

    assert!(
        matches!(tasks, Err(AdminError::Other(message)) if message.contains("invalid unix timestamp"))
    );
}

#[tokio::test]
async fn async_broker_lists_active_tasks_marks_expired_leases_orphaned() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut active = TaskMessage::from_task(&Task::new("email:active", b"payload".to_vec()));
    active.id = "active-id".to_owned();
    active.queue = "critical".to_owned();
    let executor = FakeExecutor {
        script_byte_vec_results: vec![
            vec![active.encode_to_vec(), Vec::new(), Vec::new()],
            vec![active.encode_to_vec()],
        ],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));
    let pagination = Pagination::new(0, 10).unwrap();

    let tasks = broker
        .list_active_tasks("critical", pagination)
        .await
        .unwrap();

    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].id(), "active-id");
    assert_eq!(tasks[0].state(), TaskState::Active);
    assert!(tasks[0].is_orphaned());
    assert_eq!(tasks[0].next_process_at(), None);
    assert!(broker.executor().calls.iter().any(|call| matches!(
        call,
        ExecutorCall::EvalScriptByteVec { script, keys, args }
            if *script == RedisScript::ListTasks
                && keys == &["asynq:{critical}:active".to_owned()]
                && args == &[
                    RedisArg::I64(-10),
                    RedisArg::I64(-1),
                    RedisArg::String("asynq:{critical}:t:".to_owned()),
                    RedisArg::String("1".to_owned()),
                ]
    )));
    assert!(matches!(
        &broker.executor().calls[2],
        ExecutorCall::EvalScriptByteVec { script, keys, args }
            if *script == RedisScript::ListLeaseExpired
                && keys == &["asynq:{critical}:lease".to_owned()]
                && args == &[
                    RedisArg::I64(1_699_999_970),
                    RedisArg::String("asynq:{critical}:t:".to_owned()),
                ]
    ));
}
