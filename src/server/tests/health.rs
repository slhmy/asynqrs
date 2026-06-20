use super::*;

#[test]
fn health_check_func_adapter_matches_upstream_callback_shape() {
    let results = Arc::new(StdMutex::new(Vec::new()));
    let observed = Arc::clone(&results);
    let handler = HealthCheckFunc(move |result: Result<(), String>| {
        observed.lock().unwrap().push(result);
    });

    handler.handle(Ok(()));
    HealthCheckHandler::handle(&handler, Err("redis down".to_owned()));

    assert!(format!("{handler:?}").starts_with("HealthCheckFunc"));
    assert_eq!(
        *results.lock().unwrap(),
        vec![Ok(()), Err("redis down".to_owned())]
    );
}

#[test]
fn server_health_check_func_builder_installs_callback() {
    let runtime = RecordingRuntime {
        results: Arc::new(Mutex::new(Vec::new())),
        queue_calls: Arc::new(Mutex::new(Vec::new())),
        maintenance_calls: Arc::new(Mutex::new(Vec::new())),
        shutdown_calls: Arc::new(Mutex::new(0)),
        sync_calls: Arc::new(Mutex::new(0)),
        runtime_state: None,
        stop_after_run_once: None,
        metadata_writes: Arc::new(Mutex::new(Vec::new())),
        metadata_clears: Arc::new(Mutex::new(Vec::new())),
    };

    let server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_health_check_handler(HealthCheckFunc(|_result: Result<(), String>| {}));

    assert!(server.health_check_handler.is_some());
}

#[tokio::test]
async fn health_check_handler_receives_periodic_ping_results() {
    let runtime = RecordingPingRuntime {
        ping_error: Some("redis down".to_owned()),
        ..RecordingPingRuntime::default()
    };
    let ping_calls = Arc::clone(&runtime.ping_calls);
    let handler = RecordingHealthCheckHandler::default();
    let results = Arc::clone(&handler.results);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut server = test_support::server_with_sleeper(runtime, ["critical"], TokioSleeper)
        .unwrap()
        .with_health_check_handler(handler)
        .with_health_check_interval(Duration::from_millis(1))
        .with_idle_sleep(Duration::from_millis(50));

    let handle =
        tokio::spawn(
            async move { test_support::run_until_stopped(&mut server, shutdown_rx).await },
        );
    wait_until(Duration::from_millis(50), || async {
        *ping_calls.lock().await > 0
    })
    .await;
    shutdown_tx.send(true).unwrap();
    handle.await.unwrap().unwrap();

    let results = results.lock().expect("health check results poisoned");
    assert!(
        results
            .iter()
            .any(|result| { result.as_ref().is_err_and(|error| error == "redis down") })
    );
}

#[tokio::test]
async fn run_until_stopped_logs_healthchecker_shutdown_messages() {
    let runtime = RecordingPingRuntime::default();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();
    let logger = Arc::new(RecordingLogger::default());
    let server_logger: Arc<dyn Logger> = logger.clone();
    let mut server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_health_check_handler(RecordingHealthCheckHandler::default())
            .with_logger(server_logger)
            .with_log_level(LogLevel::Debug);

    test_support::run_until_stopped(&mut server, shutdown_rx)
        .await
        .unwrap();

    let calls = logger.calls.lock().unwrap();
    assert!(calls.contains(&(
        "debug".to_owned(),
        "Healthchecker shutting down...".to_owned()
    )));
    assert!(calls.contains(&("debug".to_owned(), "Healthchecker done".to_owned())));
}
