use super::*;

#[tokio::test]
async fn async_broker_lists_aggregating_tasks_by_group() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut message = TaskMessage::from_task(&Task::new("email:aggregating", b"payload".to_vec()));
    message.id = "task-id".to_owned();
    message.queue = "critical".to_owned();
    message.group_key = "tenant-a".to_owned();
    let executor = FakeExecutor {
        script_byte_vec_results: vec![vec![
            message.encode_to_vec(),
            b"1700000120".to_vec(),
            b"group-result".to_vec(),
        ]],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));
    let pagination = Pagination::new(0, 10).unwrap();

    let tasks = broker
        .list_aggregating_tasks("critical", "tenant-a", pagination)
        .await
        .unwrap();

    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].message().id, "task-id");
    assert_eq!(tasks[0].message().group_key, "tenant-a");
    assert_eq!(tasks[0].state(), TaskState::Aggregating);
    assert_eq!(tasks[0].next_process_at(), None);
    assert_eq!(tasks[0].result(), b"group-result");
    assert!(matches!(
        &broker.executor().calls[0],
        ExecutorCall::Sismember { key, member }
            if key == "asynq:queues" && member == "critical"
    ));
    assert!(matches!(
        &broker.executor().calls[1],
        ExecutorCall::EvalScriptByteVec { script, keys, args }
            if *script == RedisScript::ListTasks
                && keys == &["asynq:{critical}:g:tenant-a".to_owned()]
                && args == &[
                    RedisArg::I64(0),
                    RedisArg::I64(9),
                    RedisArg::String("asynq:{critical}:t:".to_owned()),
                    RedisArg::String("0".to_owned()),
                ]
    ));
}

#[tokio::test]
async fn async_broker_runs_all_aggregating_tasks_by_group() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![3],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let moved = broker
        .run_all_aggregating_tasks("critical", "tenant-a")
        .await
        .unwrap();

    assert_eq!(moved, 3);
    assert!(matches!(
        &broker.executor().calls[0],
        ExecutorCall::Sismember { key, member }
            if key == "asynq:queues" && member == "critical"
    ));
    assert!(matches!(
        &broker.executor().calls[1],
        ExecutorCall::EvalScriptInt { script, keys, args }
            if *script == RedisScript::RunAllAggregatingTasks
                && keys == &[
                    "asynq:{critical}:g:tenant-a".to_owned(),
                    "asynq:{critical}:pending".to_owned(),
                    "asynq:{critical}:groups".to_owned(),
                ]
                && args == &[
                    RedisArg::String("asynq:{critical}:t:".to_owned()),
                    RedisArg::String("tenant-a".to_owned()),
                ]
    ));
}

#[tokio::test]
async fn async_broker_archives_all_aggregating_tasks_by_group() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![2],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let archived = broker
        .archive_all_aggregating_tasks("critical", "tenant-a")
        .await
        .unwrap();

    assert_eq!(archived, 2);
    assert!(matches!(
        &broker.executor().calls[0],
        ExecutorCall::Sismember { key, member }
            if key == "asynq:queues" && member == "critical"
    ));
    assert!(matches!(
        &broker.executor().calls[1],
        ExecutorCall::EvalScriptInt { script, keys, args }
            if *script == RedisScript::ArchiveAllAggregatingTasks
                && keys == &[
                    "asynq:{critical}:g:tenant-a".to_owned(),
                    "asynq:{critical}:archived".to_owned(),
                    "asynq:{critical}:groups".to_owned(),
                ]
                && args == &[
                    RedisArg::I64(1_700_000_000),
                    RedisArg::I64(1_692_224_000),
                    RedisArg::I64(10_000),
                    RedisArg::String("asynq:{critical}:t:".to_owned()),
                    RedisArg::String("tenant-a".to_owned()),
                ]
    ));
}

#[tokio::test]
async fn async_broker_archive_all_aggregating_reads_clock_after_queue_check() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let call_log = Arc::new(Mutex::new(Vec::new()));
    let executor = FakeExecutor {
        call_log: Some(Arc::clone(&call_log)),
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(
        executor,
        RecordingClock {
            now,
            call_log: Arc::clone(&call_log),
        },
    );

    broker
        .archive_all_aggregating_tasks("critical", "tenant-a")
        .await
        .unwrap();

    assert_eq!(&*call_log.lock().unwrap(), &["sismember", "clock"]);
    let ExecutorCall::EvalScriptInt { args, .. } = &broker.executor().calls[1] else {
        panic!("expected archive all aggregating script call");
    };
    assert_eq!(args[0], RedisArg::I64(1_700_000_000));
}

#[tokio::test]
async fn async_broker_deletes_all_aggregating_tasks_by_group() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![4],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let deleted = broker
        .delete_all_aggregating_tasks("critical", "tenant-a")
        .await
        .unwrap();

    assert_eq!(deleted, 4);
    assert!(matches!(
        &broker.executor().calls[0],
        ExecutorCall::Sismember { key, member }
            if key == "asynq:queues" && member == "critical"
    ));
    assert!(matches!(
        &broker.executor().calls[1],
        ExecutorCall::EvalScriptInt { script, keys, args }
            if *script == RedisScript::DeleteAllAggregatingTasks
                && keys == &[
                    "asynq:{critical}:g:tenant-a".to_owned(),
                    "asynq:{critical}:groups".to_owned(),
                ]
                && args == &[
                    RedisArg::String("asynq:{critical}:t:".to_owned()),
                    RedisArg::String("tenant-a".to_owned()),
                ]
    ));
}
