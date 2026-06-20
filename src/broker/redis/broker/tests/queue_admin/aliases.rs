use super::*;

#[tokio::test]
async fn async_broker_lists_queues_in_redis_order() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        smembers_results: vec![vec!["low".to_owned(), "critical".to_owned()]],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let queues = broker.list_queues().await.unwrap();

    assert_eq!(queues, ["low", "critical"]);
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::Smembers {
            key: "asynq:queues".to_owned(),
        }]
    );
}

#[tokio::test]
async fn async_broker_public_inspector_admin_aliases_delegate_to_existing_methods() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    let executor = FakeExecutor {
        smembers_results: vec![vec!["critical".to_owned()]],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));
    assert_eq!(broker.list_queues().await.unwrap(), ["critical"]);
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::Smembers {
            key: "asynq:queues".to_owned(),
        }]
    );

    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));
    broker.pause_queue("critical").await.unwrap();
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::SetNxI64 {
            key: "asynq:{critical}:paused".to_owned(),
            value: 1_700_000_000,
        }]
    );

    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));
    broker.unpause_queue("critical").await.unwrap();
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::Del {
            key: "asynq:{critical}:paused".to_owned(),
        }]
    );

    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));
    broker.delete_queue_force("critical").await.unwrap();
    assert!(matches!(
        &broker.executor().calls[1],
        ExecutorCall::EvalScriptInt { script, .. } if *script == RedisScript::DeleteQueueForce
    ));

    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));
    assert_eq!(broker.cluster_key_slot("critical").await.unwrap(), 12182);
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::ClusterKeySlot {
            key: "asynq:{critical}:pending".to_owned(),
        }]
    );

    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));
    broker.publish_cancel("task-id").await.unwrap();
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::Publish {
            channel: "asynq:cancel".to_owned(),
            payload: "task-id".to_owned(),
        }]
    );
}
