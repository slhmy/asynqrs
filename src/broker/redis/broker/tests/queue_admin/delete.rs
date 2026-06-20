use super::*;

#[tokio::test]
async fn async_broker_delete_queue_removes_empty_queue() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker.delete_queue("critical").await.unwrap();

    assert!(matches!(
        &broker.executor().calls[0],
        ExecutorCall::Sismember { key, member }
            if key == "asynq:queues" && member == "critical"
    ));
    let ExecutorCall::EvalScriptInt { script, keys, args } = &broker.executor().calls[1] else {
        panic!("expected delete queue script call");
    };
    assert_eq!(*script, RedisScript::DeleteQueue);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:pending".to_owned(),
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:scheduled".to_owned(),
            "asynq:{critical}:retry".to_owned(),
            "asynq:{critical}:archived".to_owned(),
            "asynq:{critical}:lease".to_owned(),
        ]
    );
    assert_eq!(args, &[RedisArg::String("asynq:{critical}:t:".to_owned())]);
    assert!(matches!(
        &broker.executor().calls[2],
        ExecutorCall::Srem { key, member }
            if key == "asynq:queues" && member == "critical"
    ));
}

#[tokio::test]
async fn async_broker_delete_queue_allows_blank_queue_name() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker.delete_queue(" ").await.unwrap();

    assert!(matches!(
        &broker.executor().calls[0],
        ExecutorCall::Sismember { key, member }
            if key == "asynq:queues" && member == " "
    ));
    let ExecutorCall::EvalScriptInt { script, keys, args } = &broker.executor().calls[1] else {
        panic!("expected delete queue script call");
    };
    assert_eq!(*script, RedisScript::DeleteQueue);
    assert_eq!(
        keys,
        &[
            "asynq:{ }:pending".to_owned(),
            "asynq:{ }:active".to_owned(),
            "asynq:{ }:scheduled".to_owned(),
            "asynq:{ }:retry".to_owned(),
            "asynq:{ }:archived".to_owned(),
            "asynq:{ }:lease".to_owned(),
        ]
    );
    assert_eq!(args, &[RedisArg::String("asynq:{ }:t:".to_owned())]);
    assert!(matches!(
        &broker.executor().calls[2],
        ExecutorCall::Srem { key, member }
            if key == "asynq:queues" && member == " "
    ));
}

#[tokio::test]
async fn async_broker_delete_queue_reports_missing_queue() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        sismember_results: vec![false],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker.delete_queue("critical").await.unwrap_err();

    assert_eq!(error, AdminError::QueueNotFound);
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::Sismember {
            key: "asynq:queues".to_owned(),
            member: "critical".to_owned(),
        }]
    );
}

#[tokio::test]
async fn async_broker_delete_queue_maps_non_empty_results() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![-1],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker.delete_queue("critical").await.unwrap_err();

    assert_eq!(error, AdminError::QueueNotEmpty);

    let executor = FakeExecutor {
        script_int_results: vec![-2],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker.delete_queue_force("critical").await.unwrap_err();

    assert_eq!(error, AdminError::QueueHasActiveTasks);
}

#[tokio::test]
async fn async_broker_delete_queue_force_uses_force_arg() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker.delete_queue_force("critical").await.unwrap();

    assert!(matches!(
        &broker.executor().calls[1],
        ExecutorCall::EvalScriptInt { script, args, .. }
            if *script == RedisScript::DeleteQueueForce
                && args == &[RedisArg::String("asynq:{critical}:t:".to_owned())]
    ));
    assert!(matches!(
        &broker.executor().calls[2],
        ExecutorCall::Srem { key, member }
            if key == "asynq:queues" && member == "critical"
    ));
}
