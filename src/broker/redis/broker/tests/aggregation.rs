use super::*;
use crate::AggregationError;

#[tokio::test]
async fn async_broker_aggregation_check_returns_new_set_id_when_ready() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let clock_now = now + Duration::from_secs(50);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(clock_now));

    let set_id = broker
        .aggregation_check_with_request(AggregationCheckRequest {
            queue: "critical",
            group: "tenant-a",
            set_id: "set-id".to_owned(),
            now,
            grace_period: Duration::from_secs(10),
            max_delay: Duration::from_secs(60),
            max_size: 100,
        })
        .await
        .unwrap();

    assert_eq!(set_id.as_deref(), Some("set-id"));
    let ExecutorCall::EvalScriptInt { script, keys, args } = &broker.executor().calls[0] else {
        panic!("expected aggregation check script call");
    };
    assert_eq!(*script, RedisScript::AggregationCheck);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:g:tenant-a".to_owned(),
            "asynq:{critical}:g:tenant-a:set-id".to_owned(),
            "asynq:{critical}:aggregation_sets".to_owned(),
            "asynq:{critical}:groups".to_owned(),
        ]
    );
    assert_eq!(
        args,
        &[
            RedisArg::I64(100),
            RedisArg::I64(60),
            RedisArg::I64(10),
            RedisArg::I64(1_700_000_170),
            RedisArg::I64(1_700_000_000),
            RedisArg::String("tenant-a".to_owned()),
        ]
    );
}

#[tokio::test]
async fn async_broker_aggregation_check_allows_blank_queue_name() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let clock_now = now + Duration::from_secs(50);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(clock_now));

    let set_id = broker
        .aggregation_check_with_request(AggregationCheckRequest {
            queue: " ",
            group: "tenant-a",
            set_id: "set-id".to_owned(),
            now,
            grace_period: Duration::from_secs(10),
            max_delay: Duration::from_secs(60),
            max_size: 100,
        })
        .await
        .unwrap();

    assert_eq!(set_id.as_deref(), Some("set-id"));
    let ExecutorCall::EvalScriptInt { script, keys, args } = &broker.executor().calls[0] else {
        panic!("expected aggregation check script call");
    };
    assert_eq!(*script, RedisScript::AggregationCheck);
    assert_eq!(
        keys,
        &[
            "asynq:{ }:g:tenant-a".to_owned(),
            "asynq:{ }:g:tenant-a:set-id".to_owned(),
            "asynq:{ }:aggregation_sets".to_owned(),
            "asynq:{ }:groups".to_owned(),
        ]
    );
    assert_eq!(args[5], RedisArg::String("tenant-a".to_owned()));
}

#[tokio::test]
async fn async_broker_aggregation_check_returns_none_when_not_ready() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![0],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let set_id = broker
        .aggregation_check_with_request(AggregationCheckRequest {
            queue: "critical",
            group: "tenant-a",
            set_id: "set-id".to_owned(),
            now,
            grace_period: Duration::from_secs(10),
            max_delay: Duration::from_secs(60),
            max_size: 100,
        })
        .await
        .unwrap();

    assert_eq!(set_id, None);
}

#[tokio::test]
async fn async_broker_reads_aggregation_set_messages_and_deadline() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    let executor = FakeExecutor {
        script_byte_vec_results: vec![vec![msg.encode_to_vec()]],
        zscore_results: vec![1_700_000_120.0],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let set = broker
        .read_aggregation_set("critical", "tenant-a", "set-id")
        .await
        .unwrap();

    assert_eq!(set.messages.as_slice(), &[msg]);
    assert_eq!(
        set.deadline(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_120)
    );
    assert_eq!(broker.executor().calls.len(), 2);
    let ExecutorCall::EvalScriptByteVec { script, keys, args } = &broker.executor().calls[0] else {
        panic!("expected read aggregation set script call");
    };
    assert_eq!(*script, RedisScript::ReadAggregationSet);
    assert_eq!(keys, &["asynq:{critical}:g:tenant-a:set-id".to_owned()]);
    assert_eq!(args, &[RedisArg::String("asynq:{critical}:t:".to_owned())]);
    assert_eq!(
        broker.executor().calls[1],
        ExecutorCall::Zscore {
            key: "asynq:{critical}:aggregation_sets".to_owned(),
            member: "asynq:{critical}:g:tenant-a:set-id".to_owned(),
        }
    );
}

#[tokio::test]
async fn async_broker_deletes_aggregation_set() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker
        .delete_aggregation_set("critical", "tenant-a", "set-id")
        .await
        .unwrap();

    let ExecutorCall::EvalScriptStatus { script, keys, args } = &broker.executor().calls[0] else {
        panic!("expected delete aggregation set script call");
    };
    assert_eq!(*script, RedisScript::DeleteAggregationSet);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:g:tenant-a:set-id".to_owned(),
            "asynq:{critical}:aggregation_sets".to_owned(),
        ]
    );
    assert_eq!(args, &[RedisArg::String("asynq:{critical}:t:".to_owned())]);
}

#[tokio::test]
async fn async_broker_reclaims_stale_aggregation_sets() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker
        .reclaim_stale_aggregation_sets_with_now("critical", now)
        .await
        .unwrap();

    let ExecutorCall::EvalScriptStatus { script, keys, args } = &broker.executor().calls[0] else {
        panic!("expected reclaim aggregation sets script call");
    };
    assert_eq!(*script, RedisScript::ReclaimStaleAggregationSets);
    assert_eq!(keys, &["asynq:{critical}:aggregation_sets".to_owned()]);
    assert_eq!(args, &[RedisArg::I64(1_700_000_000)]);
}

#[tokio::test]
async fn async_broker_aggregation_maps_executor_errors() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_error: Some(RedisExecutorError::new("connection closed")),
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker
        .aggregation_check_with_request(AggregationCheckRequest {
            queue: "critical",
            group: "tenant-a",
            set_id: "set-id".to_owned(),
            now,
            grace_period: Duration::from_secs(10),
            max_delay: Duration::from_secs(60),
            max_size: 100,
        })
        .await
        .unwrap_err();

    assert_eq!(
        error,
        AggregationError::Other("connection closed".to_owned())
    );
}
