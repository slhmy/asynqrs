use super::*;

#[tokio::test]
async fn worker_assembly_maintenance_logs_obey_log_level() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let runtime_logger: Arc<dyn Logger> = logger.clone();
    let broker = RecordingBroker {
        delete_expired_completed_error_queues: vec!["default".to_owned()],
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        |_retried: i32, _error: &HandlerError, _task: &Task| Duration::from_secs(90),
        TestClock(now),
    )
    .with_logger(runtime_logger)
    .with_log_level(LogLevel::Fatal);

    assembly
        .run_janitor_maintenance(&["default".to_owned()])
        .await
        .unwrap();

    assert!(logger.logs.lock().unwrap().is_empty());

    let logger = Arc::new(RecordingLogger::default());
    let runtime_logger: Arc<dyn Logger> = logger.clone();
    let broker = RecordingBroker {
        delete_expired_completed_error_queues: vec!["default".to_owned()],
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        |_retried: i32, _error: &HandlerError, _task: &Task| Duration::from_secs(90),
        TestClock(now),
    )
    .with_logger(runtime_logger)
    .with_log_level(LogLevel::Unspecified);

    assembly
        .run_janitor_maintenance(&["default".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        ["Failed to delete expired completed tasks from queue \"default\": cleanup down"]
    );
}

#[tokio::test]
async fn recoverer_maintenance_logs_lease_and_reclaim_errors() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let runtime_logger: Arc<dyn Logger> = logger.clone();
    let broker = RecordingBroker {
        lease_expired_error: Some(RecoverError::Other("scan down".to_owned())),
        reclaim_stale_aggregation_sets_error: Some(AggregationError::Other(
            "reclaim down".to_owned(),
        )),
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
        .run_recoverer_maintenance(&["critical".to_owned(), "default".to_owned()])
        .await
        .unwrap();

    assert_eq!(result.recovered_retried, 0);
    assert_eq!(result.recovered_archived, 0);
    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        [
            "recoverer: could not list lease expired tasks: scan down",
            "recoverer: could not reclaim stale aggregation sets in queue \"critical\": reclaim down",
            "recoverer: could not reclaim stale aggregation sets in queue \"default\": reclaim down",
        ]
    );
}

#[tokio::test]
async fn recovery_write_errors_do_not_fail_maintenance() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let broker = RecordingBroker {
        lease_expired: vec![message("retry-1"), {
            let mut message = message("archive-1");
            message.retry = 1;
            message.retried = 1;
            message
        }],
        retry_error: Some(RetryError::Other("redis down".to_owned())),
        archive_error: Some(ArchiveError::Other("redis down".to_owned())),
        deleted_expired_completed: 5,
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        |_retried: i32, _error: &HandlerError, _task: &Task| Duration::from_secs(90),
        TestClock(now),
    );

    let result = assembly
        .run_maintenance(&["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(result.recovered_retried, 0);
    assert_eq!(result.recovered_archived, 0);
    assert_eq!(result.deleted_expired_completed, 5);
    assert_eq!(assembly.broker.retried.len(), 1);
    assert_eq!(assembly.broker.archived.len(), 1);
}

#[tokio::test]
async fn maintenance_reclaim_errors_do_not_skip_cleanup() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let broker = RecordingBroker {
        deleted_expired_completed: 5,
        reclaim_stale_aggregation_sets_error: Some(AggregationError::Other(
            "reclaim down".to_owned(),
        )),
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        |_retried: i32, _error: &HandlerError, _task: &Task| Duration::from_secs(90),
        TestClock(now),
    );

    let result = assembly
        .run_maintenance(&["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(result.deleted_expired_completed, 5);
    assert_eq!(
        assembly.broker.reclaimed_stale_aggregation_sets,
        [("critical".to_owned(), now)]
    );
}

#[tokio::test]
async fn maintenance_recover_list_errors_do_not_skip_cleanup() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let broker = RecordingBroker {
        lease_expired_error: Some(RecoverError::Other("recover down".to_owned())),
        deleted_expired_completed: 5,
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        |_retried: i32, _error: &HandlerError, _task: &Task| Duration::from_secs(90),
        TestClock(now),
    );

    let result = assembly
        .run_maintenance(&["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(result.recovered_retried, 0);
    assert_eq!(result.recovered_archived, 0);
    assert_eq!(result.deleted_expired_completed, 5);
    assert!(assembly.broker.retried.is_empty());
    assert!(assembly.broker.archived.is_empty());
}
