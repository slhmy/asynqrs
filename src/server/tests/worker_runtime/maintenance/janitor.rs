use super::*;

#[tokio::test]
async fn maintenance_uses_configured_cleanup_batch_size() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let broker = RecordingBroker {
        deleted_expired_completed: 5,
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        |_retried: i32, _error: &HandlerError, _task: &Task| Duration::from_secs(90),
        TestClock(now),
    )
    .with_cleanup_batch_size(17);

    let result = assembly
        .run_maintenance(&["critical".to_owned(), "default".to_owned()])
        .await
        .unwrap();

    assert_eq!(result.deleted_expired_completed, 10);
    assert_eq!(assembly.cleanup_batch_size(), 17);
    assert_eq!(
        assembly.broker.cleanup_batches,
        [("critical".to_owned(), 17), ("default".to_owned(), 17)]
    );
}

#[tokio::test]
async fn janitor_maintenance_continues_after_queue_cleanup_error() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let runtime_logger: Arc<dyn Logger> = logger.clone();
    let broker = RecordingBroker {
        deleted_expired_completed: 5,
        delete_expired_completed_error_queues: vec!["default".to_owned()],
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        |_retried: i32, _error: &HandlerError, _task: &Task| Duration::from_secs(90),
        TestClock(now),
    )
    .with_logger(runtime_logger);

    let result = assembly
        .run_janitor_maintenance(&[
            "critical".to_owned(),
            "default".to_owned(),
            "low".to_owned(),
        ])
        .await
        .unwrap();

    assert_eq!(result.deleted_expired_completed, 10);
    assert_eq!(
        assembly.broker.cleanup_batches,
        [
            ("critical".to_owned(), DEFAULT_JANITOR_BATCH_SIZE),
            ("default".to_owned(), DEFAULT_JANITOR_BATCH_SIZE),
            ("low".to_owned(), DEFAULT_JANITOR_BATCH_SIZE)
        ]
    );
    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        ["Failed to delete expired completed tasks from queue \"default\": cleanup down"]
    );
}

#[test]
fn zero_cleanup_batch_size_uses_upstream_default() {
    let assembly = WorkerAssembly::with_parts(
        RecordingBroker::default(),
        |_task: &Task| Ok::<(), HandlerError>(()),
        |_retried: i32, _error: &HandlerError, _task: &Task| Duration::from_secs(90),
        TestClock(UNIX_EPOCH),
    )
    .with_cleanup_batch_size(0);

    assert_eq!(assembly.cleanup_batch_size(), DEFAULT_JANITOR_BATCH_SIZE);
}

#[test]
fn negative_cleanup_batch_size_is_preserved_like_upstream() {
    let assembly = WorkerAssembly::with_parts(
        RecordingBroker::default(),
        |_task: &Task| Ok::<(), HandlerError>(()),
        |_retried: i32, _error: &HandlerError, _task: &Task| Duration::from_secs(90),
        TestClock(UNIX_EPOCH),
    )
    .with_cleanup_batch_size(-1);

    assert_eq!(assembly.cleanup_batch_size(), -1);
}

#[test]
fn worker_assembly_applies_configured_janitor_batch_size() {
    let assembly = WorkerAssembly::with_parts(
        RecordingBroker::default(),
        |_task: &Task| Ok::<(), HandlerError>(()),
        |_retried: i32, _error: &HandlerError, _task: &Task| Duration::from_secs(90),
        TestClock(UNIX_EPOCH),
    );
    let config = Config {
        janitor_batch_size: 17,
        ..Config::default()
    };

    let assembly = assembly.apply_server_config(&config);

    assert_eq!(assembly.cleanup_batch_size(), 17);
}

#[test]
fn worker_assembly_preserves_negative_configured_janitor_batch_size() {
    let assembly = WorkerAssembly::with_parts(
        RecordingBroker::default(),
        |_task: &Task| Ok::<(), HandlerError>(()),
        |_retried: i32, _error: &HandlerError, _task: &Task| Duration::from_secs(90),
        TestClock(UNIX_EPOCH),
    );
    let config = Config {
        janitor_batch_size: -1,
        ..Config::default()
    };

    let assembly = assembly.apply_server_config(&config);

    assert_eq!(assembly.cleanup_batch_size(), -1);
}
