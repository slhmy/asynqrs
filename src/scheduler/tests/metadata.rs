use super::*;

#[tokio::test]
async fn run_once_logs_and_ignores_scheduler_metadata_write_errors() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let scheduler_logger: Arc<dyn Logger> = logger.clone();
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker {
            write_entries_error: Some(MetadataError::Other("redis down".to_owned())),
            ..RecordingSchedulerBroker::default()
        },
        TestClock(now),
    )
    .unwrap()
    .with_logger(scheduler_logger);
    scheduler
        .register_spec(
            "entry-id",
            Task::new("email:welcome", b"payload".to_vec()),
            "@every 1m",
        )
        .unwrap();
    scheduler.entries[0].next_enqueue_at = now;

    let run = scheduler.run_once().await.unwrap();

    assert_eq!(run.enqueued(), 1);
    assert_eq!(scheduler.broker.enqueued.len(), 1);
    assert_eq!(scheduler.broker.metadata.len(), 1);
    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        ["Scheduler could not write heartbeat data: redis down"]
    );
}

#[tokio::test]
async fn scheduler_metadata_write_warning_obeys_log_level() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let scheduler_logger: Arc<dyn Logger> = logger.clone();
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker {
            write_entries_error: Some(MetadataError::Other("redis down".to_owned())),
            ..RecordingSchedulerBroker::default()
        },
        TestClock(now),
    )
    .unwrap()
    .with_logger(scheduler_logger)
    .with_log_level(LogLevel::Error);

    scheduler.run_once().await.unwrap();

    assert!(logger.logs.lock().unwrap().is_empty());
}

#[tokio::test]
async fn scheduler_metadata_clear_logs_obey_log_level() {
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
    .with_logger(scheduler_logger)
    .with_log_level(LogLevel::Fatal);
    scheduler
        .register_spec(
            "entry-id",
            Task::new("email:welcome", b"payload".to_vec()),
            "@every 1m",
        )
        .unwrap();
    scheduler.begin_start().unwrap();

    scheduler.shutdown().await.unwrap();

    assert!(logger.logs.lock().unwrap().is_empty());
}

#[tokio::test]
async fn run_until_stopped_writes_scheduler_metadata_on_heartbeat_interval() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap()
    .with_tick_interval(Duration::from_secs(5))
    .with_heartbeat_interval(Duration::from_millis(1));
    let mut scheduler = scheduler.with_metadata_ttl(Duration::from_millis(3));
    scheduler
        .register_spec(
            "entry-a",
            Task::new("email:welcome", b"payload-a".to_vec()),
            "@every 1m",
        )
        .unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut sleeper = ShutdownAfterSleeps {
        sleeps: 0,
        shutdown_after: 5,
        shutdown: shutdown_tx,
    };

    scheduler
        .run_until_stopped(&mut sleeper, shutdown_rx)
        .await
        .unwrap();

    assert!(scheduler.broker.metadata.len() >= 2);
    assert!(
        scheduler
            .broker
            .metadata
            .iter()
            .all(|(scheduler_id, entries, ttl)| {
                scheduler_id == "scheduler-id"
                    && entries.len() == 1
                    && entries[0].0 == "entry-a"
                    && *ttl == Duration::from_millis(3)
            })
    );
}

#[tokio::test]
async fn run_until_stopped_waits_for_first_heartbeat_to_write_scheduler_metadata() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap()
    .with_tick_interval(Duration::from_millis(1))
    .with_heartbeat_interval(Duration::from_secs(60));
    scheduler
        .register_spec(
            "entry-id",
            Task::new("email:welcome", b"payload".to_vec()),
            "@every 1m",
        )
        .unwrap();
    let mut sleeper = TokioSleeper;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();

    scheduler
        .run_until_stopped(&mut sleeper, shutdown_rx)
        .await
        .unwrap();

    assert!(scheduler.broker.metadata.is_empty());
    assert_eq!(scheduler.broker.cleared, ["scheduler-id"]);
}

#[tokio::test]
async fn initial_scheduler_metadata_uses_upstream_zero_prev_enqueue_time() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap()
    .with_tick_interval(Duration::from_millis(2))
    .with_heartbeat_interval(Duration::from_millis(1));
    let mut scheduler = scheduler;
    scheduler
        .register_spec(
            "entry-id",
            Task::new("email:welcome", b"payload".to_vec()),
            "@every 1m",
        )
        .unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut sleeper = ShutdownAfterSleeps {
        sleeps: 0,
        shutdown_after: 2,
        shutdown: shutdown_tx,
    };

    scheduler
        .run_until_stopped(&mut sleeper, shutdown_rx)
        .await
        .unwrap();

    assert!(!scheduler.broker.metadata.is_empty());
    let metadata_entry = &scheduler.broker.metadata[0].1[0];
    let metadata = pb::asynq::SchedulerEntry::decode(metadata_entry.1.as_slice()).unwrap();
    assert_eq!(metadata.prev_enqueue_time, Some(go_zero_time_timestamp()));
}
