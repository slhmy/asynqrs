use super::*;

#[tokio::test]
async fn broker_ping_delegates_to_executor() {
    let mut broker = RedisBroker::with_clock(
        FakeExecutor::default(),
        TestClock(UNIX_EPOCH + Duration::from_secs(1_700_000_000)),
    );

    broker.ping().await.unwrap();

    assert_eq!(broker.executor().calls, [ExecutorCall::Ping]);
}

#[tokio::test]
async fn broker_ping_maps_executor_errors() {
    let mut broker = RedisBroker::with_clock(
        FakeExecutor {
            ping_error: Some(RedisExecutorError::new("connection closed")),
            ..FakeExecutor::default()
        },
        TestClock(UNIX_EPOCH + Duration::from_secs(1_700_000_000)),
    );

    let err = broker.ping().await.unwrap_err();

    assert!(err.to_string().contains("connection closed"));
    assert_eq!(broker.executor().calls, [ExecutorCall::Ping]);
}

#[test]
fn broker_implements_close_broker_for_redis_backed_clients() {
    let mut broker = RedisBroker::with_clock(
        FakeExecutor::default(),
        TestClock(UNIX_EPOCH + Duration::from_secs(1_700_000_000)),
    );

    CloseBroker::close(&mut broker).unwrap();

    assert_eq!(broker.executor().calls, [ExecutorCall::Close]);
}
