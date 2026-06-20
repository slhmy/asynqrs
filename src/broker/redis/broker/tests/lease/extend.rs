use super::*;

#[tokio::test]
async fn async_broker_extends_existing_lease() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    let extension = broker
        .extend_lease_with_now("critical", "task-id", now)
        .await
        .unwrap();

    assert_eq!(
        extension.expires_at(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_030)
    );
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::ZaddExistingMany {
            key: "asynq:{critical}:lease".to_owned(),
            score: 1_700_000_030,
            members: vec!["task-id".to_owned()],
        }]
    );
}

#[tokio::test]
async fn async_broker_extends_existing_leases_in_one_queue_batch() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    let extension = broker
        .extend_leases_with_now("critical", &["task-a".to_owned(), "task-b".to_owned()], now)
        .await
        .unwrap();

    assert_eq!(
        extension.expires_at(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_030)
    );
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::ZaddExistingMany {
            key: "asynq:{critical}:lease".to_owned(),
            score: 1_700_000_030,
            members: vec!["task-a".to_owned(), "task-b".to_owned()],
        }]
    );
}

#[tokio::test]
async fn async_broker_extend_lease_allows_blank_queue_name() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    let extension = broker
        .extend_lease_with_now(" ", "task-id", now)
        .await
        .unwrap();

    assert_eq!(
        extension.expires_at(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_030)
    );
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::ZaddExistingMany {
            key: "asynq:{ }:lease".to_owned(),
            score: 1_700_000_030,
            members: vec!["task-id".to_owned()],
        }]
    );
}

#[tokio::test]
async fn async_broker_reports_missing_lease_without_creating_one() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        zadd_existing_results: vec![0],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let extension = broker
        .extend_lease_with_now("critical", "task-id", now)
        .await
        .unwrap();

    assert_eq!(
        extension.expires_at(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_030)
    );
}
