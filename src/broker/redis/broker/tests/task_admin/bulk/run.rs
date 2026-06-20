use super::*;

#[tokio::test]
async fn async_broker_runs_all_zset_tasks_by_state() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![3, 2, 1],
        sismember_results: vec![true, true, true],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    assert_eq!(broker.run_all_scheduled_tasks("critical").await.unwrap(), 3);
    assert_eq!(broker.run_all_retry_tasks("critical").await.unwrap(), 2);
    assert_eq!(broker.run_all_archived_tasks("critical").await.unwrap(), 1);

    assert!(matches!(
        &broker.executor().calls[1],
        ExecutorCall::EvalScriptInt { script, keys, args }
            if *script == RedisScript::RunAllTasks
                && keys == &[
                    "asynq:{critical}:scheduled".to_owned(),
                    "asynq:{critical}:pending".to_owned(),
                ]
                && args == &[RedisArg::String("asynq:{critical}:t:".to_owned())]
    ));
    assert!(matches!(
        &broker.executor().calls[3],
        ExecutorCall::EvalScriptInt { script, keys, args }
            if *script == RedisScript::RunAllTasks
                && keys == &[
                    "asynq:{critical}:retry".to_owned(),
                    "asynq:{critical}:pending".to_owned(),
                ]
                && args == &[RedisArg::String("asynq:{critical}:t:".to_owned())]
    ));
    assert!(matches!(
        &broker.executor().calls[5],
        ExecutorCall::EvalScriptInt { script, keys, args }
            if *script == RedisScript::RunAllTasks
                && keys == &[
                    "asynq:{critical}:archived".to_owned(),
                    "asynq:{critical}:pending".to_owned(),
                ]
                && args == &[RedisArg::String("asynq:{critical}:t:".to_owned())]
    ));
}
