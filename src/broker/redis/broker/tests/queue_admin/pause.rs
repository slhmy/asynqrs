use super::*;

#[tokio::test]
async fn async_broker_pauses_queue() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker.pause_queue_with_now("critical", now).await.unwrap();

    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::SetNxI64 {
            key: "asynq:{critical}:paused".to_owned(),
            value: 1_700_000_000,
        }]
    );
}

#[tokio::test]
async fn async_broker_pause_queue_reports_already_paused() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        set_nx_results: vec![false],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker
        .pause_queue_with_now("critical", now)
        .await
        .unwrap_err();

    assert_eq!(error, AdminError::QueueAlreadyPaused);
}

#[tokio::test]
async fn async_broker_unpauses_queue() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker.unpause_queue("critical").await.unwrap();

    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::Del {
            key: "asynq:{critical}:paused".to_owned(),
        }]
    );
}
