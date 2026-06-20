use super::*;

#[tokio::test]
async fn shutdown_logs_and_ignores_scheduler_metadata_clear_errors() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let scheduler_logger: Arc<dyn Logger> = logger.clone();
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker {
            clear_entries_error: Some(MetadataError::Other("redis down".to_owned())),
            clear_history_error: Some(MetadataError::Other("history down".to_owned())),
            ..RecordingSchedulerBroker::default()
        },
        TestClock(now),
    )
    .unwrap()
    .with_logger(scheduler_logger);
    scheduler
        .register_spec(
            "entry-a",
            Task::new("email:welcome", b"payload-a".to_vec()),
            "@every 1m",
        )
        .unwrap();
    scheduler
        .register_spec(
            "entry-b",
            Task::new("email:reminder", b"payload-b".to_vec()),
            "@every 5m",
        )
        .unwrap();
    scheduler.begin_start().unwrap();

    scheduler.shutdown().await.unwrap();

    assert_eq!(scheduler.state, SchedulerState::Closed);
    assert_eq!(scheduler.broker.cleared, ["scheduler-id"]);
    assert_eq!(scheduler.broker.cleared_history, ["entry-a", "entry-b"]);
    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        [
            "Scheduler shutting down",
            "Failed to clear the scheduler entries: redis down",
            "Could not clear scheduler history for entry \"entry-a\": history down",
            "Could not clear scheduler history for entry \"entry-b\": history down",
            "Scheduler stopped",
        ]
    );
}

#[tokio::test]
async fn shutdown_logs_upstream_lifecycle_messages() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let scheduler_logger: Arc<dyn Logger> = logger.clone();
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap()
    .with_logger(scheduler_logger);
    scheduler.begin_start().unwrap();

    scheduler.shutdown().await.unwrap();

    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        ["Scheduler shutting down", "Scheduler stopped"]
    );
}

#[tokio::test]
async fn shutdown_logs_upstream_heartbeater_debug_message() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let scheduler_logger: Arc<dyn Logger> = logger.clone();
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap()
    .with_logger(scheduler_logger)
    .with_log_level(LogLevel::Debug);
    scheduler.begin_start().unwrap();

    scheduler.shutdown().await.unwrap();

    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        [
            "Scheduler shutting down",
            "Scheduler heatbeater shutting down",
            "Scheduler stopped",
        ]
    );
}

#[tokio::test]
async fn start_logs_upstream_lifecycle_messages() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let scheduler_logger: Arc<dyn Logger> = logger.clone();
    let scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap()
    .with_logger(scheduler_logger)
    .with_timezone(chrono_tz::Asia::Tokyo);

    let handle = scheduler.start().unwrap();
    let _ = handle.shutdown().await.unwrap();

    assert_eq!(
        &logger.logs.lock().unwrap()[..2],
        [
            "Scheduler starting",
            "Scheduler timezone is set to Asia/Tokyo",
        ]
    );
}

#[tokio::test]
async fn run_until_stopped_logs_upstream_shutdown_lifecycle_messages() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let scheduler_logger: Arc<dyn Logger> = logger.clone();
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap()
    .with_logger(scheduler_logger);
    let mut sleeper = TokioSleeper;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();

    scheduler
        .run_until_stopped(&mut sleeper, shutdown_rx)
        .await
        .unwrap();

    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        [
            "Scheduler starting",
            "Scheduler timezone is set to UTC",
            "Scheduler shutting down",
            "Scheduler stopped",
        ]
    );
}
