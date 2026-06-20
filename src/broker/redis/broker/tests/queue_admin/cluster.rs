use super::*;

#[tokio::test]
async fn async_broker_returns_cluster_key_slot_for_queue_pending_key() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    let keyslot = broker.cluster_key_slot("critical").await.unwrap();

    assert_eq!(keyslot, 12182);
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::ClusterKeySlot {
            key: "asynq:{critical}:pending".to_owned(),
        }]
    );
}

#[tokio::test]
async fn async_broker_cluster_key_slot_allows_empty_queue_name() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    let keyslot = broker.cluster_key_slot("").await.unwrap();

    assert_eq!(keyslot, 12182);
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::ClusterKeySlot {
            key: "asynq:{}:pending".to_owned(),
        }]
    );
}

#[tokio::test]
async fn async_broker_returns_cluster_nodes_for_queue_slot() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        cluster_key_slot_results: vec![12182],
        cluster_slots_results: vec![vec![
            RedisSlotRange::new(
                0,
                6000,
                vec![ClusterNode::new(
                    "node-a".to_owned(),
                    "127.0.0.1:7000".to_owned(),
                )],
            ),
            RedisSlotRange::new(
                6001,
                16383,
                vec![
                    ClusterNode::new("node-b".to_owned(), "127.0.0.1:7001".to_owned()),
                    ClusterNode::new("node-c".to_owned(), "127.0.0.1:7002".to_owned()),
                ],
            ),
        ]],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let nodes = broker.cluster_nodes("critical").await.unwrap();

    assert_eq!(
        nodes,
        [
            ClusterNode::new("node-b".to_owned(), "127.0.0.1:7001".to_owned()),
            ClusterNode::new("node-c".to_owned(), "127.0.0.1:7002".to_owned()),
        ]
    );
    assert_eq!(
        broker.executor().calls,
        [
            ExecutorCall::ClusterKeySlot {
                key: "asynq:{critical}:pending".to_owned(),
            },
            ExecutorCall::ClusterSlots,
        ]
    );
}

#[tokio::test]
async fn async_broker_cluster_nodes_reports_missing_slot_owner() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        cluster_key_slot_results: vec![12182],
        cluster_slots_results: vec![vec![RedisSlotRange::new(
            0,
            6000,
            vec![ClusterNode::new(
                "node-a".to_owned(),
                "127.0.0.1:7000".to_owned(),
            )],
        )]],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker.cluster_nodes("critical").await.unwrap_err();

    assert_eq!(error, AdminError::Other("nodes not found".to_owned()));
}
