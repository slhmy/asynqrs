use super::*;

#[tokio::test]
async fn run_until_stopped_logs_upstream_lifecycle_messages() {
    let runtime = RecordingPingRuntime::default();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();
    let logger = Arc::new(RecordingLogger::default());
    let server_logger: Arc<dyn Logger> = logger.clone();
    let mut server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_logger(server_logger);

    test_support::run_until_stopped(&mut server, shutdown_rx)
        .await
        .unwrap();

    let calls = logger.calls.lock().unwrap();
    assert_eq!(
        calls.as_slice(),
        [
            ("info".to_owned(), "Starting processing".to_owned()),
            ("info".to_owned(), "Starting graceful shutdown".to_owned()),
            ("info".to_owned(), "Exiting".to_owned())
        ]
    );
}

#[tokio::test]
async fn run_until_stopped_logs_runtime_done_debug_message() {
    let runtime = RecordingPingRuntime::default();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();
    let logger = Arc::new(RecordingLogger::default());
    let server_logger: Arc<dyn Logger> = logger.clone();
    let mut server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_logger(server_logger)
            .with_log_level(LogLevel::Debug);

    test_support::run_until_stopped(&mut server, shutdown_rx)
        .await
        .unwrap();

    let calls = logger.calls.lock().unwrap();
    assert_eq!(
        calls.as_slice(),
        [
            ("info".to_owned(), "Starting processing".to_owned()),
            ("info".to_owned(), "Starting graceful shutdown".to_owned()),
            ("debug".to_owned(), "Worker runtime done".to_owned()),
            ("debug".to_owned(), "Syncer shutting down...".to_owned()),
            ("debug".to_owned(), "Syncer done".to_owned()),
            ("info".to_owned(), "Exiting".to_owned())
        ]
    );
}

#[test]
fn log_level_parses_and_displays_like_upstream() {
    assert_eq!("debug".parse::<LogLevel>().unwrap(), LogLevel::Debug);
    assert_eq!("info".parse::<LogLevel>().unwrap(), LogLevel::Info);
    assert_eq!("warn".parse::<LogLevel>().unwrap(), LogLevel::Warn);
    assert_eq!("warning".parse::<LogLevel>().unwrap(), LogLevel::Warn);
    assert_eq!("error".parse::<LogLevel>().unwrap(), LogLevel::Error);
    assert_eq!("fatal".parse::<LogLevel>().unwrap(), LogLevel::Fatal);
    assert_eq!("WARNING".parse::<LogLevel>().unwrap(), LogLevel::Warn);

    assert_eq!(LogLevel::Warn.to_string(), "warn");

    let error = "trace".parse::<LogLevel>().unwrap_err();
    assert_eq!(error.value(), "trace");
    assert_eq!(error.to_string(), "asynq: unsupported log level \"trace\"");
}

#[test]
#[should_panic(expected = "asynq: unexpected log level: 0")]
fn log_level_string_panics_for_unspecified_like_upstream() {
    let _ = LogLevel::Unspecified.to_string();
}

#[test]
fn log_level_display_and_parse_match_upstream_strings() {
    let mut level = LogLevel::Info;

    assert_eq!(level.to_string(), "info");

    level = "warning".parse().unwrap();

    assert_eq!(level, LogLevel::Warn);
    assert_eq!(level.to_string(), "warn");

    let error: ParseLogLevelError = "trace".parse::<LogLevel>().unwrap_err();

    assert_eq!(level, LogLevel::Warn);
    assert_eq!(error.value(), "trace");
    assert_eq!(error.to_string(), "asynq: unsupported log level \"trace\"");
}

#[test]
fn log_level_public_constants_match_upstream_names() {
    assert_eq!(LogLevel::Unspecified as i32, 0);
    assert_eq!(LogLevel::Debug as i32, 1);
    assert_eq!(LogLevel::Info as i32, 2);
    assert_eq!(LogLevel::Warn as i32, 3);
    assert_eq!(LogLevel::Error as i32, 4);
    assert_eq!(LogLevel::Fatal as i32, 5);
}

#[test]
fn logger_trait_matches_upstream_method_boundary() {
    fn assert_logger_trait(_logger: &dyn Logger) {}

    let logger = RecordingLogger::default();
    assert_logger_trait(&logger);

    logger.debug(format_args!("debug {}", 1));
    logger.info(format_args!("info {}", 2));
    logger.warn(format_args!("warn {}", 3));
    logger.error(format_args!("error {}", 4));
    logger.fatal(format_args!("fatal {}", 5));

    let calls = logger.calls.lock().unwrap().clone();
    assert_eq!(
        calls,
        [
            ("debug".to_owned(), "debug 1".to_owned()),
            ("info".to_owned(), "info 2".to_owned()),
            ("warn".to_owned(), "warn 3".to_owned()),
            ("error".to_owned(), "error 4".to_owned()),
            ("fatal".to_owned(), "fatal 5".to_owned()),
        ]
    );
}

#[test]
fn server_log_level_defaults_and_builder_match_upstream_config() {
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
            .unwrap();

    assert_eq!(server.log_level, LogLevel::Info);

    let server = server.with_log_level(LogLevel::Debug);

    assert_eq!(server.log_level, LogLevel::Debug);

    let server = server.with_log_level(LogLevel::Unspecified);
    assert_eq!(server.log_level, LogLevel::Info);
}

#[tokio::test]
async fn run_until_stopped_logs_aggregator_shutdown_signal() {
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
    let logger = Arc::new(RecordingLogger::default());
    let server_logger: Arc<dyn Logger> = logger.clone();
    let runner = RecordingAggregationRunner::default();
    let starts = Arc::clone(&runner.starts);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut server = test_support::server_with_sleeper(runtime, ["critical"], TokioSleeper)
        .unwrap()
        .with_logger(server_logger)
        .with_log_level(LogLevel::Debug)
        .with_aggregation_runner(runner);

    let handle =
        tokio::spawn(
            async move { test_support::run_until_stopped(&mut server, shutdown_rx).await },
        );
    wait_until(Duration::from_millis(50), || async {
        *starts.lock().await == 1
    })
    .await;
    shutdown_tx.send(true).unwrap();
    handle.await.unwrap().unwrap();

    assert!(
        logger
            .calls
            .lock()
            .unwrap()
            .contains(&("debug".to_owned(), "Aggregator shutting down...".to_owned()))
    );
}

#[tokio::test]
async fn run_until_stopped_logs_subscriber_shutdown_before_aggregator_like_upstream() {
    let runtime = recording_runtime();
    let logger = Arc::new(RecordingLogger::default());
    let server_logger: Arc<dyn Logger> = logger.clone();
    let listener = RecordingCancellationListener::default();
    let runner = RecordingAggregationRunner::default();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();
    let mut server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_cancellation_listener(listener)
            .with_aggregation_runner(runner)
            .with_logger(server_logger)
            .with_log_level(LogLevel::Debug);

    test_support::run_until_stopped(&mut server, shutdown_rx)
        .await
        .unwrap();

    let calls = logger.calls.lock().unwrap();
    assert!(calls.contains(&("debug".to_owned(), "Subscriber shutting down...".to_owned())));
    assert!(calls.contains(&("debug".to_owned(), "Subscriber done".to_owned())));
    assert_before(
        &calls,
        ("debug", "Syncer done"),
        ("debug", "Subscriber shutting down..."),
    );
    assert_before(
        &calls,
        ("debug", "Subscriber shutting down..."),
        ("debug", "Subscriber done"),
    );
    assert_before(
        &calls,
        ("debug", "Subscriber done"),
        ("debug", "Aggregator shutting down..."),
    );
}

#[tokio::test]
async fn run_until_stopped_logs_healthchecker_shutdown_after_aggregator_like_upstream() {
    let runtime = recording_runtime();
    let logger = Arc::new(RecordingLogger::default());
    let server_logger: Arc<dyn Logger> = logger.clone();
    let runner = RecordingAggregationRunner::default();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();
    let mut server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_health_check_handler(RecordingHealthCheckHandler::default())
            .with_aggregation_runner(runner)
            .with_logger(server_logger)
            .with_log_level(LogLevel::Debug);

    test_support::run_until_stopped(&mut server, shutdown_rx)
        .await
        .unwrap();

    let calls = logger.calls.lock().unwrap();
    let aggregator = calls
        .iter()
        .position(|call| *call == ("debug".to_owned(), "Aggregator shutting down...".to_owned()))
        .unwrap();
    let healthchecker = calls
        .iter()
        .position(|call| {
            *call
                == (
                    "debug".to_owned(),
                    "Healthchecker shutting down...".to_owned(),
                )
        })
        .unwrap();
    assert!(aggregator < healthchecker);
}

#[tokio::test]
async fn run_until_stopped_logs_heartbeater_shutdown_after_healthchecker_like_upstream() {
    let runtime = recording_runtime();
    let logger = Arc::new(RecordingLogger::default());
    let server_logger: Arc<dyn Logger> = logger.clone();
    let runner = RecordingAggregationRunner::default();
    let metadata = ServerMetadata::new(
        "host",
        123,
        "server-id",
        b"server-info".to_vec(),
        ["worker-a"],
        Duration::from_secs(30),
    )
    .unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();
    let mut server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_server_metadata(metadata)
            .with_health_check_handler(RecordingHealthCheckHandler::default())
            .with_aggregation_runner(runner)
            .with_logger(server_logger)
            .with_log_level(LogLevel::Debug);

    test_support::run_until_stopped(&mut server, shutdown_rx)
        .await
        .unwrap();

    let calls = logger.calls.lock().unwrap();
    let aggregator = calls
        .iter()
        .position(|call| *call == ("debug".to_owned(), "Aggregator shutting down...".to_owned()))
        .unwrap();
    let healthchecker = calls
        .iter()
        .position(|call| {
            *call
                == (
                    "debug".to_owned(),
                    "Healthchecker shutting down...".to_owned(),
                )
        })
        .unwrap();
    let healthchecker_done = calls
        .iter()
        .position(|call| *call == ("debug".to_owned(), "Healthchecker done".to_owned()))
        .unwrap();
    let heartbeater = calls
        .iter()
        .position(|call| {
            *call
                == (
                    "debug".to_owned(),
                    "Heartbeater shutting down...".to_owned(),
                )
        })
        .unwrap();
    let heartbeater_done = calls
        .iter()
        .position(|call| *call == ("debug".to_owned(), "Heartbeater done".to_owned()))
        .unwrap();

    assert!(aggregator < healthchecker);
    assert!(healthchecker < healthchecker_done);
    assert!(healthchecker_done < heartbeater);
    assert!(heartbeater < heartbeater_done);
}

#[tokio::test]
async fn start_logs_upstream_lifecycle_messages_once() {
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
    let logger = Arc::new(RecordingLogger::default());
    let server_logger: Arc<dyn Logger> = logger.clone();
    let server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_worker_count(2)
            .with_logger(server_logger);

    let handle = server.start().unwrap();
    handle.shutdown().await.unwrap();

    let calls = logger.calls.lock().unwrap();
    assert_eq!(
        calls.as_slice(),
        [
            ("info".to_owned(), "Starting processing".to_owned()),
            ("info".to_owned(), "Starting graceful shutdown".to_owned()),
            ("info".to_owned(), "Exiting".to_owned())
        ]
    );
}

#[tokio::test]
async fn start_logs_healthchecker_shutdown_after_aggregator_like_upstream() {
    let runtime = recording_runtime();
    let logger = Arc::new(RecordingLogger::default());
    let server_logger: Arc<dyn Logger> = logger.clone();
    let runner = RecordingAggregationRunner::default();
    let server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_worker_count(2)
            .with_health_check_handler(RecordingHealthCheckHandler::default())
            .with_aggregation_runner(runner)
            .with_logger(server_logger)
            .with_log_level(LogLevel::Debug);

    let handle = server.start().unwrap();
    handle.shutdown().await.unwrap();

    let calls = logger.calls.lock().unwrap();
    let aggregator = calls
        .iter()
        .position(|call| *call == ("debug".to_owned(), "Aggregator shutting down...".to_owned()))
        .unwrap();
    let healthchecker = calls
        .iter()
        .position(|call| {
            *call
                == (
                    "debug".to_owned(),
                    "Healthchecker shutting down...".to_owned(),
                )
        })
        .unwrap();
    let healthchecker_done = calls
        .iter()
        .position(|call| *call == ("debug".to_owned(), "Healthchecker done".to_owned()))
        .unwrap();
    assert!(aggregator < healthchecker);
    assert!(healthchecker < healthchecker_done);
}

#[tokio::test]
async fn start_logs_heartbeater_shutdown_after_healthchecker_like_upstream() {
    let runtime = recording_runtime();
    let logger = Arc::new(RecordingLogger::default());
    let server_logger: Arc<dyn Logger> = logger.clone();
    let runner = RecordingAggregationRunner::default();
    let metadata = ServerMetadata::new(
        "host",
        123,
        "server-id",
        b"server-info".to_vec(),
        ["worker-a"],
        Duration::from_secs(30),
    )
    .unwrap();
    let server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_worker_count(2)
            .with_server_metadata(metadata)
            .with_health_check_handler(RecordingHealthCheckHandler::default())
            .with_aggregation_runner(runner)
            .with_logger(server_logger)
            .with_log_level(LogLevel::Debug);

    let handle = server.start().unwrap();
    handle.shutdown().await.unwrap();

    let calls = logger.calls.lock().unwrap();
    let aggregator = calls
        .iter()
        .position(|call| *call == ("debug".to_owned(), "Aggregator shutting down...".to_owned()))
        .unwrap();
    let healthchecker = calls
        .iter()
        .position(|call| {
            *call
                == (
                    "debug".to_owned(),
                    "Healthchecker shutting down...".to_owned(),
                )
        })
        .unwrap();
    let healthchecker_done = calls
        .iter()
        .position(|call| *call == ("debug".to_owned(), "Healthchecker done".to_owned()))
        .unwrap();
    let heartbeater = calls
        .iter()
        .position(|call| {
            *call
                == (
                    "debug".to_owned(),
                    "Heartbeater shutting down...".to_owned(),
                )
        })
        .unwrap();
    let heartbeater_done = calls
        .iter()
        .position(|call| *call == ("debug".to_owned(), "Heartbeater done".to_owned()))
        .unwrap();

    assert!(aggregator < healthchecker);
    assert!(healthchecker < healthchecker_done);
    assert!(healthchecker_done < heartbeater);
    assert!(heartbeater < heartbeater_done);
}

#[tokio::test]
async fn stop_logs_upstream_runtime_lifecycle_once() {
    let runtime = recording_runtime();
    let logger = Arc::new(RecordingLogger::default());
    let server_logger: Arc<dyn Logger> = logger.clone();
    let server = test_support::server_with_sleeper(runtime, ["critical"], TokioSleeper)
        .unwrap()
        .with_idle_sleep(Duration::from_millis(1))
        .with_logger(server_logger);

    let handle = server.start().unwrap();
    handle.stop().await.unwrap();
    handle.stop().await.unwrap();
    handle.shutdown().await.unwrap();

    let calls = logger.calls.lock().unwrap();
    assert_eq!(
        calls
            .iter()
            .filter(|call| **call == ("info".to_owned(), "Stopping worker runtime".to_owned()))
            .count(),
        1
    );
    assert_eq!(
        calls
            .iter()
            .filter(|call| **call == ("info".to_owned(), "Worker runtime stopped".to_owned()))
            .count(),
        1
    );
}

#[tokio::test]
async fn parallel_run_logs_ordered_maintenance_shutdown_messages() {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();
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
    let logger = Arc::new(RecordingLogger::default());
    let server_logger: Arc<dyn Logger> = logger.clone();
    let server = test_support::server_with_sleeper(runtime, ["critical"], TokioSleeper)
        .unwrap()
        .with_logger(server_logger)
        .with_log_level(LogLevel::Debug);

    test_support::run_until_stopped_parallel(server, 1, shutdown_rx)
        .await
        .unwrap();

    let calls = logger.calls.lock().unwrap();
    assert_eq!(
        calls
            .iter()
            .filter(|call| **call == ("debug".to_owned(), "Worker runtime done".to_owned()))
            .count(),
        1
    );
    assert!(calls.contains(&("debug".to_owned(), "Forwarder done".to_owned())));
    assert!(calls.contains(&("debug".to_owned(), "Recoverer done".to_owned())));
    assert!(calls.contains(&("debug".to_owned(), "Janitor done".to_owned())));
    assert!(calls.contains(&("debug".to_owned(), "Forwarder shutting down...".to_owned())));
    assert!(calls.contains(&("debug".to_owned(), "Recoverer shutting down...".to_owned())));
    assert!(calls.contains(&("debug".to_owned(), "Syncer shutting down...".to_owned())));
    assert!(calls.contains(&("debug".to_owned(), "Janitor shutting down...".to_owned())));
    assert!(calls.contains(&("debug".to_owned(), "Syncer done".to_owned())));
    assert_before(
        &calls,
        ("debug", "Forwarder shutting down..."),
        ("debug", "Forwarder done"),
    );
    assert_before(
        &calls,
        ("debug", "Forwarder done"),
        ("debug", "Recoverer shutting down..."),
    );
    assert_before(
        &calls,
        ("debug", "Recoverer done"),
        ("debug", "Syncer shutting down..."),
    );
    assert_before(
        &calls,
        ("debug", "Syncer done"),
        ("debug", "Janitor shutting down..."),
    );
    assert_before(
        &calls,
        ("debug", "Janitor shutting down..."),
        ("debug", "Janitor done"),
    );
}

#[tokio::test]
async fn start_logs_subscriber_shutdown_between_syncer_and_janitor_like_upstream() {
    let runtime = recording_runtime();
    let logger = Arc::new(RecordingLogger::default());
    let server_logger: Arc<dyn Logger> = logger.clone();
    let listener = RecordingCancellationListener::default();
    let server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_worker_count(1)
            .with_cancellation_listener(listener)
            .with_logger(server_logger)
            .with_log_level(LogLevel::Debug);

    let handle = server.start().unwrap();
    handle.shutdown().await.unwrap();

    let calls = logger.calls.lock().unwrap();
    assert!(calls.contains(&("debug".to_owned(), "Subscriber shutting down...".to_owned())));
    assert!(calls.contains(&("debug".to_owned(), "Subscriber done".to_owned())));
    assert_before(
        &calls,
        ("debug", "Syncer done"),
        ("debug", "Subscriber shutting down..."),
    );
    assert_before(
        &calls,
        ("debug", "Subscriber done"),
        ("debug", "Janitor shutting down..."),
    );
}

fn assert_before(calls: &[(String, String)], first: (&str, &str), second: (&str, &str)) {
    let first = calls
        .iter()
        .position(|call| call.0 == first.0 && call.1 == first.1)
        .unwrap();
    let second = calls
        .iter()
        .position(|call| call.0 == second.0 && call.1 == second.1)
        .unwrap();
    assert!(first < second);
}
