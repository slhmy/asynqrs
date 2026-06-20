use super::*;

#[tokio::test]
async fn shutdown_requeues_active_task() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let runtime_logger: Arc<dyn Logger> = logger.clone();
    let broker = RecordingBroker::default();
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        DefaultRetryDelay,
        TestClock(now),
    )
    .with_logger(runtime_logger);
    start_active_test_worker(
        &mut assembly,
        message("task-id"),
        now,
        None,
        now + Duration::from_secs(30),
    );

    assembly.shutdown().await.unwrap();

    assert_eq!(assembly.broker.requeued, ["task-id"]);
    assert!(assembly.active_task_id.is_none());
    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        [
            "Waiting for all workers to finish...",
            "Quitting worker. task id=task-id",
            "Pushed task id=task-id back to queue",
            "All workers have finished",
        ]
    );
}

#[tokio::test]
async fn shutdown_unregisters_active_task_cancellation() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let broker = RecordingBroker::default();
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        DefaultRetryDelay,
        TestClock(now),
    );
    start_active_test_worker(
        &mut assembly,
        message("task-id"),
        now,
        None,
        now + Duration::from_secs(30),
    );
    let canceller = assembly.canceller();
    let _receiver = canceller.register("task-id");

    assert!(canceller.cancel("task-id"));

    assembly.shutdown().await.unwrap();

    assert!(!canceller.cancel("task-id"));
    assert_eq!(assembly.broker.requeued, ["task-id"]);
}

#[tokio::test]
async fn shutdown_ignores_requeue_errors_for_active_task() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let runtime_logger: Arc<dyn Logger> = logger.clone();
    let broker = RecordingBroker {
        requeue_error: Some(RequeueError::Other("redis down".to_owned())),
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        DefaultRetryDelay,
        TestClock(now),
    )
    .with_logger(runtime_logger);
    start_active_test_worker(
        &mut assembly,
        message("task-id"),
        now,
        None,
        now + Duration::from_secs(30),
    );

    assembly.shutdown().await.unwrap();

    assert_eq!(assembly.broker.requeued, ["task-id"]);
    assert!(assembly.active_task_id.is_none());
    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        [
            "Waiting for all workers to finish...",
            "Quitting worker. task id=task-id",
            "Could not push task id=task-id back to queue: redis down",
            "All workers have finished",
        ]
    );
}

#[tokio::test]
async fn shutdown_logs_upstream_lifecycle_debug_messages() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let runtime_logger: Arc<dyn Logger> = logger.clone();
    let broker = RecordingBroker::default();
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        DefaultRetryDelay,
        TestClock(now),
    )
    .with_logger(runtime_logger)
    .with_log_level(LogLevel::Debug);

    assembly.shutdown().await.unwrap();

    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        [
            "Worker runtime shutting down...",
            "Waiting for all workers to finish...",
            "All workers have finished",
        ]
    );
}

#[tokio::test]
async fn shutdown_skips_requeue_after_lease_expires() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let broker = RecordingBroker::default();
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        DefaultRetryDelay,
        TestClock(now),
    );
    start_active_test_worker(
        &mut assembly,
        message("task-id"),
        now,
        None,
        now - Duration::from_secs(1),
    );

    assembly.shutdown().await.unwrap();

    assert!(assembly.broker.requeued.is_empty());
    assert!(assembly.active_task_id.is_none());
}

#[tokio::test]
async fn shutdown_drains_pending_complete_sync_before_exit() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message("task-id")))],
        complete_error: Some(CompleteError::Other("redis down".to_owned())),
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        DefaultRetryDelay,
        TestClock(now),
    );

    run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();
    assert_eq!(assembly.pending_sync_count(), 1);

    assembly.broker_mut().complete_error = None;
    assembly.shutdown().await.unwrap();

    assert_eq!(assembly.broker.completed, ["task-id", "task-id"]);
    assert_eq!(assembly.pending_sync_count(), 0);
}

#[tokio::test]
async fn shutdown_drops_failed_pending_sync_after_final_attempt_like_upstream() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let runtime_logger: Arc<dyn Logger> = logger.clone();
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message("task-id")))],
        complete_error: Some(CompleteError::Other("redis down".to_owned())),
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        DefaultRetryDelay,
        TestClock(now),
    )
    .with_logger(runtime_logger);

    run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();
    assembly.shutdown().await.unwrap();

    assert_eq!(assembly.broker.completed, ["task-id", "task-id"]);
    assert_eq!(assembly.pending_sync_count(), 0);
    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        [
            "Could not remove task id=task-id type=\"email:welcome\" from \"asynq:{critical}:active\" err: redis down; Will retry syncing",
            "Waiting for all workers to finish...",
            "All workers have finished",
            "Could not remove task id=task-id type=\"email:welcome\" from \"asynq:{critical}:active\" err: redis down",
        ]
    );
}
