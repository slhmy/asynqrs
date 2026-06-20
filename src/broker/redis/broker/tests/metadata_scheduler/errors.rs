use super::*;

#[tokio::test]
async fn async_broker_metadata_maps_executor_errors() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_error: Some(RedisExecutorError::new("connection closed")),
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker
        .write_server_state(
            "host",
            123,
            "server-id",
            b"server-info".to_vec(),
            [b"worker-a".to_vec()],
            Duration::from_secs(10),
        )
        .await
        .unwrap_err();

    assert_eq!(error, MetadataError::Other("connection closed".to_owned()));
}
