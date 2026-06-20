use super::*;

#[tokio::test]
async fn async_broker_deletes_task() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker.delete_task("critical", "task-id").await.unwrap();

    assert!(matches!(
        &broker.executor().calls[0],
        ExecutorCall::Sismember { key, member }
            if key == "asynq:queues" && member == "critical"
    ));
    let ExecutorCall::EvalScriptInt { script, keys, args } = &broker.executor().calls[1] else {
        panic!("expected delete task script call");
    };
    assert_eq!(*script, RedisScript::DeleteTask);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:groups".to_owned(),
        ]
    );
    assert_eq!(
        args,
        &[
            RedisArg::String("task-id".to_owned()),
            RedisArg::String("asynq:{critical}:".to_owned()),
            RedisArg::String("asynq:{critical}:g:".to_owned()),
        ]
    );
}

#[tokio::test]
async fn async_broker_delete_task_maps_script_results() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![0],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker
        .delete_task("critical", "missing-id")
        .await
        .unwrap_err();

    assert_eq!(error, AdminError::TaskNotFound);

    let executor = FakeExecutor {
        script_int_results: vec![-1],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker
        .delete_task("critical", "active-id")
        .await
        .unwrap_err();

    assert_eq!(error, AdminError::CannotDeleteActiveTask);
}

#[tokio::test]
async fn async_broker_runs_task() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker.run_task("critical", "task-id").await.unwrap();

    assert!(matches!(
        &broker.executor().calls[0],
        ExecutorCall::Sismember { key, member }
            if key == "asynq:queues" && member == "critical"
    ));
    let ExecutorCall::EvalScriptInt { script, keys, args } = &broker.executor().calls[1] else {
        panic!("expected run task script call");
    };
    assert_eq!(*script, RedisScript::RunTask);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:pending".to_owned(),
            "asynq:{critical}:groups".to_owned(),
        ]
    );
    assert_eq!(
        args,
        &[
            RedisArg::String("task-id".to_owned()),
            RedisArg::String("asynq:{critical}:".to_owned()),
            RedisArg::String("asynq:{critical}:g:".to_owned()),
        ]
    );
}

#[tokio::test]
async fn async_broker_run_task_maps_script_results() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![0],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker.run_task("critical", "missing-id").await.unwrap_err();

    assert_eq!(error, AdminError::TaskNotFound);

    let executor = FakeExecutor {
        script_int_results: vec![-1],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker.run_task("critical", "active-id").await.unwrap_err();

    assert_eq!(error, AdminError::TaskAlreadyRunning);

    let executor = FakeExecutor {
        script_int_results: vec![-2],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker.run_task("critical", "pending-id").await.unwrap_err();

    assert_eq!(error, AdminError::TaskAlreadyPending);
}

#[tokio::test]
async fn async_broker_archives_task() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker.archive_task("critical", "task-id").await.unwrap();

    assert!(matches!(
        &broker.executor().calls[0],
        ExecutorCall::Sismember { key, member }
            if key == "asynq:queues" && member == "critical"
    ));
    let ExecutorCall::EvalScriptInt { script, keys, args } = &broker.executor().calls[1] else {
        panic!("expected archive task script call");
    };
    assert_eq!(*script, RedisScript::ArchiveTask);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:archived".to_owned(),
            "asynq:{critical}:groups".to_owned(),
        ]
    );
    assert_eq!(
        args,
        &[
            RedisArg::String("task-id".to_owned()),
            RedisArg::I64(1_700_000_000),
            RedisArg::I64(1_692_224_000),
            RedisArg::I64(10_000),
            RedisArg::String("asynq:{critical}:".to_owned()),
            RedisArg::String("asynq:{critical}:g:".to_owned()),
        ]
    );
}

#[tokio::test]
async fn async_broker_archive_task_reads_clock_after_queue_check() {
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

    broker.archive_task("critical", "task-id").await.unwrap();

    assert_eq!(&*call_log.lock().unwrap(), &["sismember", "clock"]);
    let ExecutorCall::EvalScriptInt { args, .. } = &broker.executor().calls[1] else {
        panic!("expected archive task script call");
    };
    assert_eq!(args[1], RedisArg::I64(1_700_000_000));
}

#[tokio::test]
async fn async_broker_archive_task_maps_script_results() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![0],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker
        .archive_task("critical", "missing-id")
        .await
        .unwrap_err();

    assert_eq!(error, AdminError::TaskNotFound);

    let executor = FakeExecutor {
        script_int_results: vec![-1],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker
        .archive_task("critical", "archived-id")
        .await
        .unwrap_err();

    assert_eq!(error, AdminError::TaskAlreadyArchived);

    let executor = FakeExecutor {
        script_int_results: vec![-2],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker
        .archive_task("critical", "active-id")
        .await
        .unwrap_err();

    assert_eq!(error, AdminError::CannotArchiveActiveTask);
}
