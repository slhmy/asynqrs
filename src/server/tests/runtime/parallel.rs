use super::*;

#[tokio::test]
async fn parallel_run_manages_maintenance_once_for_all_workers() {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let runtime = RecordingRuntime {
        results: Arc::new(Mutex::new(vec![Ok(WorkerRun::NoProcessableTask)])),
        stop_after_run_once: Some(shutdown_tx),
        ..recording_runtime()
    };
    let maintenance_calls = Arc::clone(&runtime.maintenance_calls);
    let server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_idle_sleep(Duration::from_millis(1));

    test_support::run_until_stopped_parallel(server, 3, shutdown_rx)
        .await
        .unwrap();

    assert!(maintenance_calls.lock().await.len() <= 1);
}

#[tokio::test]
async fn configured_parallel_run_uses_server_worker_count() {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();
    let runtime = recording_runtime();
    let shutdown_calls = Arc::clone(&runtime.shutdown_calls);
    let server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_worker_count(3);

    test_support::run_until_stopped_configured_parallel(server, shutdown_rx)
        .await
        .unwrap();

    assert_eq!(*shutdown_calls.lock().await, 3);
}

#[tokio::test]
async fn start_method_uses_configured_worker_count() {
    let runtime = recording_runtime();
    let shutdown_calls = Arc::clone(&runtime.shutdown_calls);
    let server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_worker_count(2);

    let handle = server.start().unwrap();
    handle.shutdown().await.unwrap();

    assert_eq!(*shutdown_calls.lock().await, 2);
}

#[tokio::test]
async fn lowercase_start_returns_handle_for_shutdown() {
    let runtime = recording_runtime();
    let shutdown_calls = Arc::clone(&runtime.shutdown_calls);
    let server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_worker_count(1);

    let handle = server.start().unwrap();
    let summary = handle.shutdown().await.unwrap();

    assert_eq!(summary.processed(), 0);
    assert_eq!(*shutdown_calls.lock().await, 1);
}

#[tokio::test]
async fn start_matches_upstream_server_state_errors() {
    let mut active = test_support::server_with_sleeper(
        recording_runtime(),
        ["critical"],
        RecordingSleeper::default(),
    )
    .unwrap();
    active.state = ServerState::Active;
    let error = active.start().unwrap_err();
    assert_eq!(error, ServerError::AlreadyRunning);
}

#[tokio::test]
async fn start_handle_ping_matches_upstream_server_ping_boundary() {
    let runtime = RecordingPingRuntime::default();
    let ping_calls = Arc::clone(&runtime.ping_calls);
    let server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_worker_count(1);

    let handle = server.start().unwrap();
    handle.ping().await.unwrap();
    handle.shutdown().await.unwrap();

    assert_eq!(*ping_calls.lock().await, 1);
}

#[tokio::test]
async fn start_handle_ping_reports_runtime_errors() {
    let runtime = RecordingPingRuntime {
        ping_error: Some("redis down".to_owned()),
        ..RecordingPingRuntime::default()
    };
    let ping_calls = Arc::clone(&runtime.ping_calls);
    let server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_worker_count(1);

    let handle = server.start().unwrap();
    let error = handle.ping().await.unwrap_err();
    handle.shutdown().await.unwrap();

    assert_eq!(error, ServerError::Ping("redis down".to_owned()));
    assert_eq!(*ping_calls.lock().await, 1);
}

#[tokio::test]
async fn stop_method_stops_polling_before_shutdown() {
    let runtime = recording_runtime();
    let queue_calls = Arc::clone(&runtime.queue_calls);
    let shutdown_calls = Arc::clone(&runtime.shutdown_calls);
    let server = test_support::server_with_sleeper(runtime, ["critical"], TokioSleeper)
        .unwrap()
        .with_idle_sleep(Duration::from_millis(1));

    let handle = server.start().unwrap();
    assert!(!handle.is_stopped());
    wait_until(Duration::from_millis(50), || async {
        !queue_calls.lock().await.is_empty()
    })
    .await;
    handle.stop().await.unwrap();
    assert!(handle.is_stopped());
    let calls_after_stop = queue_calls.lock().await.len();
    tokio::time::sleep(Duration::from_millis(10)).await;

    assert_eq!(queue_calls.lock().await.len(), calls_after_stop);
    assert_eq!(*shutdown_calls.lock().await, 0);

    handle.stop().await.unwrap();
    assert_eq!(queue_calls.lock().await.len(), calls_after_stop);

    handle.shutdown().await.unwrap();

    assert_eq!(*shutdown_calls.lock().await, 1);
}

#[tokio::test]
async fn stop_method_keeps_syncer_running_until_shutdown() {
    let runtime = RecordingRuntime {
        runtime_state: Some(pending_sync_runtime_state()),
        ..recording_runtime()
    };
    let queue_calls = Arc::clone(&runtime.queue_calls);
    let sync_calls = Arc::clone(&runtime.sync_calls);
    let server = test_support::server_with_sleeper(runtime, ["critical"], TokioSleeper)
        .unwrap()
        .with_worker_count(1)
        .with_idle_sleep(Duration::from_millis(1))
        .with_syncer_interval(Duration::from_millis(1));

    let handle = server.start().unwrap();
    wait_until(Duration::from_millis(50), || async {
        !queue_calls.lock().await.is_empty()
    })
    .await;
    handle.stop().await.unwrap();
    wait_until(Duration::from_millis(50), || async {
        *sync_calls.lock().await >= 1
    })
    .await;

    assert!(handle.is_stopped());
    assert!(*sync_calls.lock().await >= 1);

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_keeps_syncer_running_until_worker_shutdown_like_upstream() {
    let (run_started_tx, mut run_started_rx) = watch::channel(false);
    let (finish_run_tx, finish_run_rx) = watch::channel(false);
    let shutdown_calls = Arc::new(Mutex::new(0));
    let sync_calls = Arc::new(Mutex::new(0));
    let runtime = GracefulShutdownRuntime {
        run_started: run_started_tx,
        finish_run: finish_run_rx,
        shutdown_calls: Arc::clone(&shutdown_calls),
        sync_calls: Arc::clone(&sync_calls),
        runtime_state: Some(pending_sync_runtime_state()),
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let server = test_support::server_with_sleeper(runtime, ["critical"], TokioSleeper)
        .unwrap()
        .with_worker_count(1)
        .with_syncer_interval(Duration::from_millis(1))
        .with_shutdown_timeout(Duration::from_secs(1));

    let handle = tokio::spawn(async move {
        test_support::run_until_stopped_configured_parallel(server, shutdown_rx).await
    });
    while !*run_started_rx.borrow() {
        run_started_rx.changed().await.unwrap();
    }
    shutdown_tx.send(true).unwrap();
    wait_until(Duration::from_millis(50), || async {
        *sync_calls.lock().await >= 1
    })
    .await;

    assert_eq!(*shutdown_calls.lock().await, 0);
    assert!(*sync_calls.lock().await >= 1);

    finish_run_tx.send(true).unwrap();
    handle.await.unwrap().unwrap();

    assert_eq!(*shutdown_calls.lock().await, 1);
}

#[tokio::test]
async fn shutdown_runs_syncer_final_retry_like_upstream() {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();
    let runtime = RecordingRuntime {
        runtime_state: Some(pending_sync_runtime_state()),
        ..recording_runtime()
    };
    let sync_calls = Arc::clone(&runtime.sync_calls);
    let server = test_support::server_with_sleeper(runtime, ["critical"], TokioSleeper)
        .unwrap()
        .with_worker_count(1)
        .with_syncer_interval(Duration::from_secs(60));

    test_support::run_until_stopped_configured_parallel(server, shutdown_rx)
        .await
        .unwrap();

    assert_eq!(*sync_calls.lock().await, 1);
}

#[tokio::test]
async fn shutdown_stops_forwarder_before_waiting_for_workers_like_upstream() {
    let (run_started_tx, mut run_started_rx) = watch::channel(false);
    let (finish_run_tx, finish_run_rx) = watch::channel(false);
    let shutdown_calls = Arc::new(Mutex::new(0));
    let sync_calls = Arc::new(Mutex::new(0));
    let runtime = GracefulShutdownRuntime {
        run_started: run_started_tx,
        finish_run: finish_run_rx,
        shutdown_calls: Arc::clone(&shutdown_calls),
        sync_calls,
        runtime_state: None,
    };
    let logger = Arc::new(RecordingLogger::default());
    let server_logger: Arc<dyn Logger> = logger.clone();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let server = test_support::server_with_sleeper(runtime, ["critical"], TokioSleeper)
        .unwrap()
        .with_worker_count(1)
        .with_logger(server_logger)
        .with_log_level(LogLevel::Debug)
        .with_shutdown_timeout(Duration::from_secs(1));

    let handle = tokio::spawn(async move {
        test_support::run_until_stopped_configured_parallel(server, shutdown_rx).await
    });
    while !*run_started_rx.borrow() {
        run_started_rx.changed().await.unwrap();
    }
    shutdown_tx.send(true).unwrap();
    wait_until(Duration::from_millis(50), || async {
        logger
            .calls
            .lock()
            .unwrap()
            .contains(&("debug".to_owned(), "Forwarder done".to_owned()))
    })
    .await;

    assert_eq!(*shutdown_calls.lock().await, 0);
    let calls = logger.calls.lock().unwrap().clone();
    let forwarder_done = calls
        .iter()
        .position(|call| *call == ("debug".to_owned(), "Forwarder done".to_owned()))
        .unwrap();
    assert!(
        calls[..forwarder_done]
            .contains(&("debug".to_owned(), "Forwarder shutting down...".to_owned()))
    );
    assert!(
        !calls[..=forwarder_done].contains(&("debug".to_owned(), "Worker runtime done".to_owned()))
    );
    finish_run_tx.send(true).unwrap();
    handle.await.unwrap().unwrap();

    assert_eq!(*shutdown_calls.lock().await, 1);
}

#[tokio::test]
async fn parallel_run_shuts_down_runtime_after_worker_error() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let runtime = RecordingRuntime {
        results: Arc::new(Mutex::new(vec![Err(ProcessingError::TimeOverflow(
            "worker run",
        ))])),
        ..recording_runtime()
    };
    let shutdown_calls = Arc::clone(&runtime.shutdown_calls);
    let server = test_support::server_with_sleeper(runtime, ["critical"], TokioSleeper)
        .unwrap()
        .with_worker_count(2)
        .with_syncer_interval(Duration::from_millis(1))
        .with_forwarder_interval(Duration::from_millis(1))
        .with_recoverer_interval(Duration::from_millis(1))
        .with_janitor_interval(Duration::from_millis(1));

    let error = tokio::time::timeout(
        Duration::from_millis(100),
        test_support::run_until_stopped_configured_parallel(server, shutdown_rx),
    )
    .await
    .expect("server should stop after worker error")
    .unwrap_err();

    assert_eq!(
        error,
        ServerError::Processing(ProcessingError::TimeOverflow("worker run"))
    );
    assert_eq!(*shutdown_calls.lock().await, 2);
}

#[tokio::test]
async fn maintenance_errors_do_not_stop_parallel_server() {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let runtime = FlakyMaintenanceRuntime {
        results: Arc::new(Mutex::new(Vec::new())),
        recoverer_results: Arc::new(Mutex::new(vec![Err(ProcessingError::Recover(
            crate::RecoverError::Other("recover down".to_owned()),
        ))])),
        recoverer_calls: Arc::new(Mutex::new(0)),
        shutdown_calls: Arc::new(Mutex::new(0)),
        stop_after_run_once: None,
    };
    let recoverer_calls = Arc::clone(&runtime.recoverer_calls);
    let shutdown_calls = Arc::clone(&runtime.shutdown_calls);
    let logger = Arc::new(RecordingLogger::default());
    let server_logger: Arc<dyn Logger> = logger.clone();
    let server = test_support::server_with_sleeper(runtime, ["critical"], TokioSleeper)
        .unwrap()
        .with_logger(server_logger)
        .with_idle_sleep(Duration::from_millis(50))
        .with_recoverer_interval(Duration::from_millis(5));

    let handle = tokio::spawn(async move {
        test_support::run_until_stopped_parallel(server, 2, shutdown_rx).await
    });
    wait_until(Duration::from_millis(50), || async {
        *recoverer_calls.lock().await >= 1
    })
    .await;
    shutdown_tx.send(true).unwrap();
    handle.await.unwrap().unwrap();

    assert!(*recoverer_calls.lock().await >= 1);
    assert_eq!(*shutdown_calls.lock().await, 2);
    assert!(logger.calls.lock().unwrap().contains(&(
        "warn".to_owned(),
        "recoverer: could not list lease expired tasks: recover down".to_owned()
    )));
}

#[tokio::test]
async fn parallel_run_owns_cancellation_listener_once() {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let runtime = RecordingRuntime {
        results: Arc::new(Mutex::new(vec![Ok(WorkerRun::NoProcessableTask)])),
        stop_after_run_once: Some(shutdown_tx),
        ..recording_runtime()
    };
    let listener = RecordingCancellationListener::default();
    let starts = Arc::clone(&listener.starts);
    let stops = Arc::clone(&listener.stops);
    let server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_cancellation_listener(listener);

    test_support::run_until_stopped_parallel(server, 3, shutdown_rx)
        .await
        .unwrap();

    assert_eq!(*starts.lock().await, 1);
    assert_eq!(*stops.lock().await, 1);
}

#[tokio::test]
async fn parallel_run_owns_aggregation_runner_once() {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let runtime = RecordingRuntime {
        results: Arc::new(Mutex::new(vec![Ok(WorkerRun::NoProcessableTask)])),
        stop_after_run_once: Some(shutdown_tx),
        ..recording_runtime()
    };
    let runner = RecordingAggregationRunner::default();
    let starts = Arc::clone(&runner.starts);
    let stops = Arc::clone(&runner.stops);
    let server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_aggregation_runner(runner);

    test_support::run_until_stopped_parallel(server, 3, shutdown_rx)
        .await
        .unwrap();

    assert_eq!(*starts.lock().await, 1);
    assert_eq!(*stops.lock().await, 1);
}

#[tokio::test]
async fn parallel_run_stops_cancellation_listener_before_aggregator_like_upstream() {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let runtime = RecordingRuntime {
        results: Arc::new(Mutex::new(vec![Ok(WorkerRun::NoProcessableTask)])),
        stop_after_run_once: Some(shutdown_tx),
        ..recording_runtime()
    };
    let events = Arc::new(Mutex::new(Vec::new()));
    let listener = RecordingCancellationListener {
        events: Arc::clone(&events),
        ..RecordingCancellationListener::default()
    };
    let runner = RecordingAggregationRunner {
        events: Arc::clone(&events),
        ..RecordingAggregationRunner::default()
    };
    let server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_cancellation_listener(listener)
            .with_aggregation_runner(runner);

    test_support::run_until_stopped_parallel(server, 3, shutdown_rx)
        .await
        .unwrap();

    let events = events.lock().await;
    let listener_stop = events
        .iter()
        .position(|event| *event == "listener-stop")
        .unwrap();
    let aggregator_stop = events
        .iter()
        .position(|event| *event == "aggregator-stop")
        .unwrap();
    assert!(listener_stop < aggregator_stop);
}

#[tokio::test]
async fn parallel_run_stops_cancellation_listener_before_janitor_like_upstream() {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let runtime = RecordingRuntime {
        results: Arc::new(Mutex::new(vec![Ok(WorkerRun::NoProcessableTask)])),
        stop_after_run_once: Some(shutdown_tx),
        ..recording_runtime()
    };
    let events = Arc::new(StdMutex::new(Vec::new()));
    let listener = OrderedCancellationListener {
        events: Arc::clone(&events),
    };
    let logger = Arc::new(OrderedShutdownLogger {
        events: Arc::clone(&events),
    });
    let server_logger: Arc<dyn Logger> = logger;
    let server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_cancellation_listener(listener)
            .with_logger(server_logger)
            .with_log_level(LogLevel::Debug);

    test_support::run_until_stopped_parallel(server, 3, shutdown_rx)
        .await
        .unwrap();

    let events = events.lock().unwrap();
    let listener_stop = events
        .iter()
        .position(|event| event == "listener-stop")
        .unwrap();
    let janitor_shutdown = events
        .iter()
        .position(|event| event == "Janitor shutting down...")
        .unwrap();
    assert!(listener_stop < janitor_shutdown);
}

#[tokio::test]
async fn rejects_zero_parallel_workers() {
    let server = test_support::server_new(recording_runtime(), ["critical"]).unwrap();
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    let error = test_support::run_until_stopped_parallel(server, 0, shutdown_rx)
        .await
        .unwrap_err();

    assert_eq!(error, ServerError::EmptyWorkerCount);
}

#[derive(Debug, Clone)]
struct OrderedCancellationListener {
    events: Arc<StdMutex<Vec<String>>>,
}

impl CancellationListener for OrderedCancellationListener {
    fn run_until_stopped(
        &self,
        mut shutdown: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<Result<usize, ServerError>> {
        let events = Arc::clone(&self.events);
        tokio::spawn(async move {
            loop {
                if *shutdown.borrow() {
                    break;
                }
                if shutdown.changed().await.is_err() {
                    break;
                }
            }
            events.lock().unwrap().push("listener-stop".to_owned());
            Ok(0)
        })
    }
}

struct OrderedShutdownLogger {
    events: Arc<StdMutex<Vec<String>>>,
}

impl Logger for OrderedShutdownLogger {
    fn debug(&self, args: std::fmt::Arguments<'_>) {
        self.events.lock().unwrap().push(args.to_string());
    }

    fn info(&self, _args: std::fmt::Arguments<'_>) {}

    fn warn(&self, _args: std::fmt::Arguments<'_>) {}

    fn error(&self, _args: std::fmt::Arguments<'_>) {}

    fn fatal(&self, _args: std::fmt::Arguments<'_>) {}
}
