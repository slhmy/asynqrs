use super::*;

#[tokio::test]
async fn async_broker_public_bulk_inspector_aliases_delegate_to_existing_methods() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));
    assert_eq!(broker.run_all_scheduled_tasks("critical").await.unwrap(), 1);
    assert!(matches!(
        &broker.executor().calls[1],
        ExecutorCall::EvalScriptInt { script, keys, .. }
            if *script == RedisScript::RunAllTasks
                && keys[0] == "asynq:{critical}:scheduled"
    ));

    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));
    assert_eq!(
        broker.archive_all_pending_tasks("critical").await.unwrap(),
        1
    );
    assert!(matches!(
        &broker.executor().calls[1],
        ExecutorCall::EvalScriptInt { script, keys, .. }
            if *script == RedisScript::ArchiveAllPendingTasks
                && keys[0] == "asynq:{critical}:pending"
    ));

    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));
    assert_eq!(
        broker.delete_all_completed_tasks("critical").await.unwrap(),
        1
    );
    assert!(matches!(
        &broker.executor().calls[1],
        ExecutorCall::EvalScriptInt { script, keys, .. }
            if *script == RedisScript::DeleteAllTasks
                && keys[0] == "asynq:{critical}:completed"
    ));

    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));
    assert_eq!(
        broker
            .run_all_aggregating_tasks("critical", "tenant-a")
            .await
            .unwrap(),
        1
    );
    assert!(matches!(
        &broker.executor().calls[1],
        ExecutorCall::EvalScriptInt { script, keys, .. }
            if *script == RedisScript::RunAllAggregatingTasks
                && keys[0] == "asynq:{critical}:g:tenant-a"
    ));

    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));
    assert_eq!(
        broker
            .archive_all_aggregating_tasks("critical", "tenant-a")
            .await
            .unwrap(),
        1
    );
    assert!(matches!(
        &broker.executor().calls[1],
        ExecutorCall::EvalScriptInt { script, keys, .. }
            if *script == RedisScript::ArchiveAllAggregatingTasks
                && keys[0] == "asynq:{critical}:g:tenant-a"
    ));

    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));
    assert_eq!(
        broker
            .delete_all_aggregating_tasks("critical", "tenant-a")
            .await
            .unwrap(),
        1
    );
    assert!(matches!(
        &broker.executor().calls[1],
        ExecutorCall::EvalScriptInt { script, keys, .. }
            if *script == RedisScript::DeleteAllAggregatingTasks
                && keys[0] == "asynq:{critical}:g:tenant-a"
    ));
}
