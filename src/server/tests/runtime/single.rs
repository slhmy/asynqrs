use super::*;

#[tokio::test]
async fn run_until_stopped_rejects_restart_after_shutdown() {
    let runtime = RecordingPingRuntime::default();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();
    let mut server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap();

    test_support::run_until_stopped(&mut server, shutdown_rx)
        .await
        .unwrap();
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let error = test_support::run_until_stopped(&mut server, shutdown_rx)
        .await
        .unwrap_err();

    assert_eq!(error, ServerError::Closed);
    assert_eq!(error.to_string(), "asynq: Server closed");
    assert!(error.is_closed());
    assert!(ServerError::Closed.is_closed());
    assert!(!ServerError::EmptyQueueList.is_closed());
}

#[tokio::test]
async fn run_method_matches_upstream_name() {
    let runtime = RecordingPingRuntime::default();
    let mut server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap();

    let run = server.run();

    std::mem::drop(run);
    assert_eq!(server.state(), ServerState::New);
}

#[tokio::test]
async fn shutdown_closes_owned_server_connection_like_upstream_new_server() {
    let runtime = CloseTrackingRuntime::default();
    let close_calls = Arc::clone(&runtime.close_calls);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();
    let mut server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap();

    test_support::run_until_stopped(&mut server, shutdown_rx)
        .await
        .unwrap();

    assert_eq!(*close_calls.lock().await, 1);
}

#[tokio::test]
async fn shutdown_leaves_shared_server_connection_open_like_upstream_from_redis_client() {
    let runtime = CloseTrackingRuntime::default();
    let close_calls = Arc::clone(&runtime.close_calls);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();
    let mut server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_shared_connection();

    test_support::run_until_stopped(&mut server, shutdown_rx)
        .await
        .unwrap();

    assert_eq!(*close_calls.lock().await, 0);
}

#[tokio::test]
async fn run_until_stopped_matches_upstream_server_start_state_errors() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut active = test_support::server_with_sleeper(
        RecordingPingRuntime::default(),
        ["critical"],
        RecordingSleeper::default(),
    )
    .unwrap();
    active.state = ServerState::Active;
    let error = test_support::run_until_stopped(&mut active, shutdown_rx)
        .await
        .unwrap_err();
    assert_eq!(error, ServerError::AlreadyRunning);
    assert_eq!(error.to_string(), "asynq: the server is already running");
}

#[tokio::test]
async fn runs_until_shutdown_and_records_summary() {
    let runtime = RecordingRuntime {
        results: Arc::new(Mutex::new(vec![
            Ok(WorkerRun::Completed {
                task_id: "task-id".to_owned(),
            }),
            Ok(WorkerRun::LeaseExpired {
                task_id: "expired-id".to_owned(),
            }),
            Ok(WorkerRun::NoProcessableTask),
        ])),
        ..recording_runtime()
    };
    let sleeper = RecordingSleeper::default();
    let durations = Arc::clone(&sleeper.durations);
    let maintenance_calls = Arc::clone(&runtime.maintenance_calls);
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let server = test_support::server_with_sleeper(runtime, ["critical"], sleeper)
        .unwrap()
        .with_idle_sleep(Duration::from_millis(5))
        .with_maintenance_interval(Duration::from_millis(1));

    let summary = tokio::time::timeout(Duration::from_millis(100), async {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut server = server;
        let handle =
            tokio::spawn(
                async move { test_support::run_until_stopped(&mut server, shutdown_rx).await },
            );
        wait_until(Duration::from_millis(50), || async {
            !durations.lock().await.is_empty()
        })
        .await;
        wait_until(Duration::from_millis(50), || async {
            maintenance_calls.lock().await.len() >= 4
        })
        .await;
        shutdown_tx.send(true).unwrap();
        handle.await.unwrap()
    })
    .await
    .unwrap()
    .unwrap();

    assert_eq!(summary.processed(), 2);
    assert_eq!(summary.completed(), 1);
    assert_eq!(summary.lease_expired(), 1);
    assert!(summary.forwarded_scheduled() >= 1);
    assert!(summary.forwarded_retry() >= 2);
    assert!(summary.recovered_retried() >= 3);
    assert!(summary.recovered_archived() >= 4);
    assert!(summary.deleted_expired_completed() >= 5);
    assert!(summary.idle_polls() >= 1);
    assert!(
        durations
            .lock()
            .await
            .iter()
            .all(|duration| *duration >= Duration::from_micros(2_500)
                && *duration < Duration::from_micros(7_500))
    );
    drop(shutdown_rx);
}

#[tokio::test]
async fn single_worker_runs_maintenance_on_interval() {
    let runtime = recording_runtime();
    let maintenance_calls = Arc::clone(&runtime.maintenance_calls);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut server = test_support::server_with_sleeper(runtime, ["critical"], TokioSleeper)
        .unwrap()
        .with_idle_sleep(Duration::from_millis(50))
        .with_maintenance_interval(Duration::from_millis(5));

    let handle =
        tokio::spawn(
            async move { test_support::run_until_stopped(&mut server, shutdown_rx).await },
        );
    wait_until(Duration::from_millis(50), || async {
        maintenance_calls.lock().await.len() >= 5
    })
    .await;
    shutdown_tx.send(true).unwrap();
    let summary = handle.await.unwrap().unwrap();

    assert!(summary.forwarded_scheduled() >= 2);
    assert!(maintenance_calls.lock().await.len() >= 2);
}

#[tokio::test]
async fn stop_signal_pauses_single_worker_polling_until_shutdown() {
    let (stop_tx, stop_rx) = watch::channel(false);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let runtime = RecordingRuntime {
        stop_after_run_once: Some(stop_tx.clone()),
        runtime_state: Some(pending_sync_runtime_state()),
        ..recording_runtime()
    };
    let queue_calls = Arc::clone(&runtime.queue_calls);
    let shutdown_calls = Arc::clone(&runtime.shutdown_calls);
    let sync_calls = Arc::clone(&runtime.sync_calls);
    let mut server = test_support::server_with_sleeper(runtime, ["critical"], TokioSleeper)
        .unwrap()
        .with_idle_sleep(Duration::from_millis(50))
        .with_syncer_interval(Duration::from_millis(1));

    let handle = tokio::spawn(async move {
        server
            .run_until_stopped_with_stop(stop_rx, shutdown_rx)
            .await
    });
    wait_until(Duration::from_millis(50), || async {
        !queue_calls.lock().await.is_empty()
    })
    .await;
    wait_until(Duration::from_millis(50), || async { *stop_tx.borrow() }).await;
    let calls_after_stop = queue_calls.lock().await.len();
    wait_until(Duration::from_millis(50), || async {
        *sync_calls.lock().await >= 1
    })
    .await;

    assert_eq!(queue_calls.lock().await.len(), calls_after_stop);
    assert_eq!(*shutdown_calls.lock().await, 0);

    shutdown_tx.send(true).unwrap();
    handle.await.unwrap().unwrap();

    assert_eq!(*shutdown_calls.lock().await, 1);
}

#[tokio::test]
async fn run_until_stopped_shuts_down_after_runtime_error() {
    let metadata = ServerMetadata::new(
        "host".to_owned(),
        42,
        "server-id".to_owned(),
        b"server-info".to_vec(),
        Vec::<String>::new(),
        Duration::from_secs(10),
    )
    .unwrap();
    let runtime = RecordingRuntime {
        results: Arc::new(Mutex::new(vec![Err(ProcessingError::TimeOverflow(
            "worker run",
        ))])),
        ..recording_runtime()
    };
    let shutdown_calls = Arc::clone(&runtime.shutdown_calls);
    let metadata_clears = Arc::clone(&runtime.metadata_clears);
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut server = test_support::server_with_sleeper(runtime, ["critical"], TokioSleeper)
        .unwrap()
        .with_server_metadata(metadata.clone());

    let error = tokio::time::timeout(
        Duration::from_millis(100),
        test_support::run_until_stopped(&mut server, shutdown_rx),
    )
    .await
    .expect("server should stop after runtime error")
    .unwrap_err();

    assert_eq!(
        error,
        ServerError::Processing(ProcessingError::TimeOverflow("worker run"))
    );
    assert_eq!(*shutdown_calls.lock().await, 1);
    assert_eq!(metadata_clears.lock().await.as_slice(), &[metadata]);
}

#[tokio::test]
async fn maintenance_errors_do_not_stop_single_worker_server() {
    let runtime = FlakyMaintenanceRuntime {
        results: Arc::new(Mutex::new(Vec::new())),
        recoverer_results: Arc::new(Mutex::new(vec![
            Err(ProcessingError::Recover(crate::RecoverError::Other(
                "recover down".to_owned(),
            ))),
            Ok(ServerMaintenanceRun::new(0, 0, 3, 4, 0)),
        ])),
        recoverer_calls: Arc::new(Mutex::new(0)),
        shutdown_calls: Arc::new(Mutex::new(0)),
        stop_after_run_once: None,
    };
    let recoverer_calls = Arc::clone(&runtime.recoverer_calls);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let logger = Arc::new(RecordingLogger::default());
    let server_logger: Arc<dyn Logger> = logger.clone();
    let mut server = test_support::server_with_sleeper(runtime, ["critical"], TokioSleeper)
        .unwrap()
        .with_logger(server_logger)
        .with_idle_sleep(Duration::from_millis(50))
        .with_recoverer_interval(Duration::from_millis(5));

    let handle =
        tokio::spawn(
            async move { test_support::run_until_stopped(&mut server, shutdown_rx).await },
        );
    wait_until(Duration::from_millis(50), || async {
        *recoverer_calls.lock().await >= 2
    })
    .await;
    shutdown_tx.send(true).unwrap();
    let summary = handle.await.unwrap().unwrap();

    assert_eq!(summary.recovered_retried(), 3);
    assert_eq!(summary.recovered_archived(), 4);
    assert!(logger.calls.lock().unwrap().contains(&(
        "warn".to_owned(),
        "recoverer: could not list lease expired tasks: recover down".to_owned()
    )));
}

#[tokio::test]
async fn run_until_stopped_owns_cancellation_listener() {
    let runtime = recording_runtime();
    let listener = RecordingCancellationListener::default();
    let starts = Arc::clone(&listener.starts);
    let stops = Arc::clone(&listener.stops);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut server = test_support::server_with_sleeper(runtime, ["critical"], TokioSleeper)
        .unwrap()
        .with_cancellation_listener(listener);

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

    assert_eq!(*starts.lock().await, 1);
    assert_eq!(*stops.lock().await, 1);
}

#[tokio::test]
async fn run_until_stopped_owns_aggregation_runner() {
    let runtime = recording_runtime();
    let runner = RecordingAggregationRunner::default();
    let starts = Arc::clone(&runner.starts);
    let stops = Arc::clone(&runner.stops);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut server = test_support::server_with_sleeper(runtime, ["critical"], TokioSleeper)
        .unwrap()
        .with_aggregation_runner(runner);

    assert!(server.aggregation_runner.is_some());
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

    assert_eq!(*starts.lock().await, 1);
    assert_eq!(*stops.lock().await, 1);
}

#[tokio::test]
async fn run_until_stopped_stops_cancellation_listener_before_aggregator_like_upstream() {
    let runtime = recording_runtime();
    let events = Arc::new(Mutex::new(Vec::new()));
    let listener = RecordingCancellationListener {
        events: Arc::clone(&events),
        ..RecordingCancellationListener::default()
    };
    let runner = RecordingAggregationRunner {
        events: Arc::clone(&events),
        ..RecordingAggregationRunner::default()
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();
    let mut server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_cancellation_listener(listener)
            .with_aggregation_runner(runner);

    test_support::run_until_stopped(&mut server, shutdown_rx)
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
