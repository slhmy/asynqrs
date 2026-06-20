use super::*;

#[tokio::test]
async fn async_broker_deletes_all_tasks_by_state() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![5, 4, 3, 2, 1],
        sismember_results: vec![true, true, true, true, true],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    assert_eq!(
        broker.delete_all_pending_tasks("critical").await.unwrap(),
        5
    );
    assert_eq!(
        broker.delete_all_scheduled_tasks("critical").await.unwrap(),
        4
    );
    assert_eq!(broker.delete_all_retry_tasks("critical").await.unwrap(), 3);
    assert_eq!(
        broker.delete_all_archived_tasks("critical").await.unwrap(),
        2
    );
    assert_eq!(
        broker.delete_all_completed_tasks("critical").await.unwrap(),
        1
    );

    assert!(matches!(
        &broker.executor().calls[1],
        ExecutorCall::EvalScriptInt { script, keys, args }
            if *script == RedisScript::DeleteAllPendingTasks
                && keys == &["asynq:{critical}:pending".to_owned()]
                && args == &[RedisArg::String("asynq:{critical}:t:".to_owned())]
    ));
    assert!(matches!(
        &broker.executor().calls[3],
        ExecutorCall::EvalScriptInt { script, keys, args }
            if *script == RedisScript::DeleteAllTasks
                && keys == &["asynq:{critical}:scheduled".to_owned()]
                && args == &[RedisArg::String("asynq:{critical}:t:".to_owned())]
    ));
    assert!(matches!(
        &broker.executor().calls[5],
        ExecutorCall::EvalScriptInt { script, keys, args }
            if *script == RedisScript::DeleteAllTasks
                && keys == &["asynq:{critical}:retry".to_owned()]
                && args == &[RedisArg::String("asynq:{critical}:t:".to_owned())]
    ));
    assert!(matches!(
        &broker.executor().calls[7],
        ExecutorCall::EvalScriptInt { script, keys, args }
            if *script == RedisScript::DeleteAllTasks
                && keys == &["asynq:{critical}:archived".to_owned()]
                && args == &[RedisArg::String("asynq:{critical}:t:".to_owned())]
    ));
    assert!(matches!(
        &broker.executor().calls[9],
        ExecutorCall::EvalScriptInt { script, keys, args }
            if *script == RedisScript::DeleteAllTasks
                && keys == &["asynq:{critical}:completed".to_owned()]
                && args == &[RedisArg::String("asynq:{critical}:t:".to_owned())]
    ));
}
