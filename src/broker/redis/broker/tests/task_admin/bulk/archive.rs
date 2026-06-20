use super::*;

#[tokio::test]
async fn async_broker_archives_all_tasks_by_state() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![4, 3, 2],
        sismember_results: vec![true, true, true],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    assert_eq!(
        broker
            .archive_all_pending_tasks_with_now("critical", now)
            .await
            .unwrap(),
        4
    );
    assert_eq!(
        broker
            .archive_all_scheduled_tasks_with_now("critical", now)
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        broker
            .archive_all_retry_tasks_with_now("critical", now)
            .await
            .unwrap(),
        2
    );

    assert!(matches!(
        &broker.executor().calls[1],
        ExecutorCall::EvalScriptInt { script, keys, args }
            if *script == RedisScript::ArchiveAllPendingTasks
                && keys == &[
                    "asynq:{critical}:pending".to_owned(),
                    "asynq:{critical}:archived".to_owned(),
                ]
                && args == &[
                    RedisArg::I64(1_700_000_000),
                    RedisArg::I64(1_692_224_000),
                    RedisArg::I64(10_000),
                    RedisArg::String("asynq:{critical}:t:".to_owned()),
                ]
    ));
    assert!(matches!(
        &broker.executor().calls[3],
        ExecutorCall::EvalScriptInt { script, keys, args }
            if *script == RedisScript::ArchiveAllTasks
                && keys == &[
                    "asynq:{critical}:scheduled".to_owned(),
                    "asynq:{critical}:archived".to_owned(),
                ]
                && args == &[
                    RedisArg::I64(1_700_000_000),
                    RedisArg::I64(1_692_224_000),
                    RedisArg::I64(10_000),
                    RedisArg::String("asynq:{critical}:t:".to_owned()),
                ]
    ));
    assert!(matches!(
        &broker.executor().calls[5],
        ExecutorCall::EvalScriptInt { script, keys, args }
            if *script == RedisScript::ArchiveAllTasks
                && keys == &[
                    "asynq:{critical}:retry".to_owned(),
                    "asynq:{critical}:archived".to_owned(),
                ]
                && args == &[
                    RedisArg::I64(1_700_000_000),
                    RedisArg::I64(1_692_224_000),
                    RedisArg::I64(10_000),
                    RedisArg::String("asynq:{critical}:t:".to_owned()),
                ]
    ));
}

#[tokio::test]
async fn async_broker_archive_all_tasks_reads_clock_after_queue_check() {
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
        .archive_all_scheduled_tasks("critical")
        .await
        .unwrap();

    assert_eq!(&*call_log.lock().unwrap(), &["sismember", "clock"]);
    let ExecutorCall::EvalScriptInt { args, .. } = &broker.executor().calls[1] else {
        panic!("expected archive all tasks script call");
    };
    assert_eq!(args[0], RedisArg::I64(1_700_000_000));
}
