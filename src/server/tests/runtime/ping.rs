use super::*;

#[tokio::test]
async fn ping_delegates_to_runtime_before_shutdown() {
    let runtime = RecordingPingRuntime::default();
    let ping_calls = Arc::clone(&runtime.ping_calls);
    let mut server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap();

    server.ping().await.unwrap();

    assert_eq!(*ping_calls.lock().await, 1);
}

#[tokio::test]
async fn ping_method_matches_upstream_name() {
    let runtime = RecordingPingRuntime::default();
    let ping_calls = Arc::clone(&runtime.ping_calls);
    let mut server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap();

    server.ping().await.unwrap();

    assert_eq!(*ping_calls.lock().await, 1);
}

#[tokio::test]
async fn ping_reports_runtime_errors() {
    let runtime = RecordingPingRuntime {
        ping_error: Some("redis down".to_owned()),
        ..RecordingPingRuntime::default()
    };
    let ping_calls = Arc::clone(&runtime.ping_calls);
    let mut server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap();

    let error = server.ping().await.unwrap_err();

    assert_eq!(error, ServerError::Ping("redis down".to_owned()));
    assert_eq!(error.to_string(), "redis down");
    assert_eq!(*ping_calls.lock().await, 1);
}

#[tokio::test]
async fn ping_returns_ok_after_server_is_closed() {
    let runtime = RecordingPingRuntime {
        ping_error: Some("redis down".to_owned()),
        ..RecordingPingRuntime::default()
    };
    let ping_calls = Arc::clone(&runtime.ping_calls);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();
    let mut server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap();

    test_support::run_until_stopped(&mut server, shutdown_rx)
        .await
        .unwrap();
    assert_eq!(server.state(), ServerState::Closed);
    server.ping().await.unwrap();

    assert_eq!(*ping_calls.lock().await, 0);
}
