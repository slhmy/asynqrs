use super::*;

#[tokio::test]
async fn start_runs_in_background_until_handle_shutdown() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap()
    .with_tick_interval(Duration::from_millis(1));
    scheduler
        .register_spec(
            "entry-id",
            Task::new("email:welcome", b"payload".to_vec()),
            "@every 1m",
        )
        .unwrap();

    let handle = scheduler.start().unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;
    let run = handle.shutdown().await.unwrap();

    assert_eq!(run.enqueued(), 0);
}

#[tokio::test]
async fn start_alias_runs_in_background_until_handle_shutdown() {
    let scheduler = Scheduler::new("scheduler-id", RecordingSchedulerBroker::default())
        .unwrap()
        .with_tick_interval(Duration::from_millis(1));

    let handle = scheduler.start().unwrap();
    let run = handle.shutdown().await.unwrap();

    assert_eq!(run.enqueued(), 0);
}

#[tokio::test]
async fn start_handle_ping_matches_upstream_scheduler_ping_boundary() {
    let scheduler = Scheduler::new("scheduler-id", RecordingSchedulerBroker::default())
        .unwrap()
        .with_tick_interval(Duration::from_millis(1));

    let handle = scheduler.start().unwrap();
    handle.ping().await.unwrap();
    let run = handle.shutdown().await.unwrap();

    assert_eq!(run.enqueued(), 0);
}

#[tokio::test]
async fn start_handle_ping_reports_broker_errors() {
    let scheduler = Scheduler::new(
        "scheduler-id",
        RecordingSchedulerBroker {
            ping_error: Some("redis down".to_owned()),
            ..RecordingSchedulerBroker::default()
        },
    )
    .unwrap()
    .with_tick_interval(Duration::from_millis(1));

    let handle = scheduler.start().unwrap();
    let err = handle.ping().await.unwrap_err();
    let run = handle.shutdown().await.unwrap();

    assert_eq!(err, SchedulerError::Ping("redis down".to_owned()));
    assert_eq!(run.enqueued(), 0);
}

#[tokio::test]
async fn start_handle_registers_entries_while_background_scheduler_runs() {
    let scheduler = Scheduler::new("scheduler-id", RecordingSchedulerBroker::default())
        .unwrap()
        .with_tick_interval(Duration::from_millis(1));

    let handle = scheduler.start().unwrap();
    handle
        .register(
            "entry-id",
            Task::new("email:welcome", b"payload".to_vec()),
            Duration::from_millis(1),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let run = handle.shutdown().await.unwrap();

    assert!(run.enqueued() > 0);
}

#[tokio::test]
async fn start_handle_registers_generated_entry_ids_while_running() {
    let scheduler = Scheduler::new("scheduler-id", RecordingSchedulerBroker::default())
        .unwrap()
        .with_tick_interval(Duration::from_millis(1));

    let handle = scheduler.start().unwrap();
    let entry_id = handle
        .register_spec_with_generated_id(
            Task::new("email:welcome", b"payload".to_vec()),
            "@every 1h",
        )
        .await
        .unwrap();

    assert!(uuid::Uuid::parse_str(&entry_id).is_ok());
    handle.unregister(&entry_id).await.unwrap();
    let run = handle.shutdown().await.unwrap();

    assert_eq!(run.enqueued(), 0);
}

#[tokio::test]
async fn start_handle_unregisters_entries_while_background_scheduler_runs() {
    let scheduler = Scheduler::new("scheduler-id", RecordingSchedulerBroker::default())
        .unwrap()
        .with_tick_interval(Duration::from_millis(1));

    let handle = scheduler.start().unwrap();
    handle
        .register_spec(
            "entry-id",
            Task::new("email:welcome", b"payload".to_vec()),
            "@every 1h",
        )
        .await
        .unwrap();

    handle.unregister("entry-id").await.unwrap();
    let err = handle.unregister("entry-id").await.unwrap_err();
    let run = handle.shutdown().await.unwrap();

    assert_eq!(err, SchedulerError::EntryNotFound("entry-id".to_owned()));
    assert_eq!(run.enqueued(), 0);
}

#[tokio::test]
async fn start_handle_returns_registration_errors_from_background_scheduler() {
    let scheduler = Scheduler::new("scheduler-id", RecordingSchedulerBroker::default())
        .unwrap()
        .with_tick_interval(Duration::from_millis(1));

    let handle = scheduler.start().unwrap();
    let err = handle
        .register_spec(
            "entry-id",
            Task::new("email:welcome", b"payload".to_vec()),
            "not-a-cron-spec",
        )
        .await
        .unwrap_err();
    let run = handle.shutdown().await.unwrap();

    assert_eq!(
        err,
        SchedulerError::UnsupportedSpec("not-a-cron-spec".to_owned())
    );
    assert_eq!(run.enqueued(), 0);
}

#[tokio::test]
async fn start_returns_state_errors_before_spawning() {
    let mut active = Scheduler::with_clock(
        "active",
        RecordingSchedulerBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap();
    active.state = SchedulerState::Active;
    let err = active.start().unwrap_err();
    assert_eq!(err, SchedulerError::AlreadyRunning);
    assert_eq!(err.to_string(), "asynq: the scheduler is already running");

    let mut closed = Scheduler::with_clock(
        "closed",
        RecordingSchedulerBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap();
    closed.state = SchedulerState::Closed;
    let err = closed.start().unwrap_err();
    assert_eq!(err, SchedulerError::AlreadyStopped);
    assert_eq!(
        err.to_string(),
        "asynq: the scheduler has already been stopped"
    );
}
