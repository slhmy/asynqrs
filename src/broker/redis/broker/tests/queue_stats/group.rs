use super::*;

#[tokio::test]
async fn async_broker_reads_group_stats() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_value_results: vec![redis::Value::Array(vec![
            redis_bulk("tenant-b"),
            redis::Value::Int(3),
            redis_bulk("tenant-a"),
            redis::Value::Int(1),
        ])],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let stats = broker.group_stats("critical").await.unwrap();

    assert_eq!(stats.len(), 2);
    assert_eq!(stats[0].group(), "tenant-b");
    assert_eq!(stats[0].size(), 3);
    assert_eq!(stats[1].group(), "tenant-a");
    assert_eq!(stats[1].size(), 1);
    assert!(matches!(
        &broker.executor().calls[0],
        ExecutorCall::EvalScriptValue { script, keys, args }
            if *script == RedisScript::GroupStats
                && keys == &["asynq:{critical}:groups".to_owned()]
                && args == &[RedisArg::String("asynq:{critical}:g:".to_owned())]
    ));
}

#[tokio::test]
async fn async_broker_reads_group_stats_allows_blank_queue_name() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_value_results: vec![redis::Value::Array(Vec::new())],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let stats = broker.group_stats(" ").await.unwrap();

    assert!(stats.is_empty());
    assert!(matches!(
        &broker.executor().calls[0],
        ExecutorCall::EvalScriptValue { script, keys, args }
            if *script == RedisScript::GroupStats
                && keys == &["asynq:{ }:groups".to_owned()]
                && args == &[RedisArg::String("asynq:{ }:g:".to_owned())]
    ));
}

#[tokio::test]
async fn async_broker_lists_aggregation_groups_for_runtime() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        smembers_results: vec![vec!["tenant-b".to_owned(), "tenant-a".to_owned()]],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let groups = AggregationBroker::list_aggregation_groups(&mut broker, "critical")
        .await
        .unwrap();

    assert_eq!(groups, ["tenant-b".to_owned(), "tenant-a".to_owned()]);
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::Smembers {
            key: "asynq:{critical}:groups".to_owned(),
        }]
    );
}

#[tokio::test]
async fn async_broker_lists_aggregation_groups_allows_blank_queue_name() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        smembers_results: vec![vec!["tenant-a".to_owned()]],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let groups = AggregationBroker::list_aggregation_groups(&mut broker, " ")
        .await
        .unwrap();

    assert_eq!(groups, ["tenant-a".to_owned()]);
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::Smembers {
            key: "asynq:{ }:groups".to_owned(),
        }]
    );
}
