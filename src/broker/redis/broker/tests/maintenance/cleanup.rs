use super::*;
use crate::CleanupError;

#[tokio::test]
async fn async_broker_deletes_expired_completed_tasks_until_empty() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![100, 5, 0],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let result = broker
        .delete_expired_completed_tasks_with_now("critical", now, 100)
        .await
        .unwrap();

    assert_eq!(result, 105);
    assert_eq!(broker.executor().calls.len(), 3);
    let ExecutorCall::EvalScriptInt { script, keys, args } = &broker.executor().calls[0] else {
        panic!("expected cleanup script call");
    };
    assert_eq!(*script, RedisScript::DeleteExpiredCompletedTasks);
    assert_eq!(keys, &["asynq:{critical}:completed".to_owned()]);
    assert_eq!(
        args,
        &[
            RedisArg::I64(1_700_000_000),
            RedisArg::String("asynq:{critical}:t:".to_owned()),
            RedisArg::I64(100),
        ]
    );
}

#[tokio::test]
async fn async_broker_delete_expired_completed_tasks_allows_zero_batch_size() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![0],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let result = broker
        .delete_expired_completed_tasks_with_now("critical", now, 0)
        .await
        .unwrap();

    assert_eq!(result, 0);
    assert_eq!(broker.executor().calls.len(), 1);
    let ExecutorCall::EvalScriptInt { script, keys, args } = &broker.executor().calls[0] else {
        panic!("expected cleanup script call");
    };
    assert_eq!(*script, RedisScript::DeleteExpiredCompletedTasks);
    assert_eq!(keys, &["asynq:{critical}:completed".to_owned()]);
    assert_eq!(
        args,
        &[
            RedisArg::I64(1_700_000_000),
            RedisArg::String("asynq:{critical}:t:".to_owned()),
            RedisArg::I64(0),
        ]
    );
}

#[tokio::test]
async fn async_broker_delete_expired_completed_tasks_allows_blank_queue_name() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![0],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let result = broker
        .delete_expired_completed_tasks_with_now(" ", now, 100)
        .await
        .unwrap();

    assert_eq!(result, 0);
    assert_eq!(broker.executor().calls.len(), 1);
    let ExecutorCall::EvalScriptInt { script, keys, args } = &broker.executor().calls[0] else {
        panic!("expected cleanup script call");
    };
    assert_eq!(*script, RedisScript::DeleteExpiredCompletedTasks);
    assert_eq!(keys, &["asynq:{ }:completed".to_owned()]);
    assert_eq!(
        args,
        &[
            RedisArg::I64(1_700_000_000),
            RedisArg::String("asynq:{ }:t:".to_owned()),
            RedisArg::I64(100),
        ]
    );
}

#[tokio::test]
async fn async_broker_delete_expired_completed_tasks_maps_executor_errors() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_error: Some(RedisExecutorError::new("connection closed")),
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker
        .delete_expired_completed_tasks_batch_with_now("critical", now, 100)
        .await
        .unwrap_err();

    assert_eq!(error, CleanupError::Other("connection closed".to_owned()));
}

#[tokio::test]
async fn deletes_expired_completed_tasks_until_empty() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![100, 5, 0],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let result = broker
        .delete_expired_completed_tasks("critical", 100)
        .await
        .unwrap();

    assert_eq!(result, 105);
    assert_eq!(broker.executor().calls.len(), 3);
    let ExecutorCall::EvalScriptInt { script, keys, args } = &broker.executor().calls[0] else {
        panic!("expected cleanup script call");
    };
    assert_eq!(*script, RedisScript::DeleteExpiredCompletedTasks);
    assert_eq!(keys, &["asynq:{critical}:completed".to_owned()]);
    assert_eq!(
        args,
        &[
            RedisArg::I64(1_700_000_000),
            RedisArg::String("asynq:{critical}:t:".to_owned()),
            RedisArg::I64(100),
        ]
    );
}
