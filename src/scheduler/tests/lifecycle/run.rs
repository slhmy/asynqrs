use super::*;

#[tokio::test]
async fn run_until_stopped_clears_scheduler_entries_and_history() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap();
    let mut scheduler = scheduler.with_tick_interval(Duration::from_millis(1));
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
            "*/5 * * * *",
        )
        .unwrap();
    let mut sleeper = TokioSleeper;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();

    scheduler
        .run_until_stopped(&mut sleeper, shutdown_rx)
        .await
        .unwrap();

    assert_eq!(scheduler.broker.cleared, ["scheduler-id"]);
    assert_eq!(scheduler.broker.cleared_history, ["entry-a", "entry-b"]);
    assert_eq!(scheduler.broker.close_calls, 1);
}

#[tokio::test]
async fn run_until_stopped_marks_scheduler_closed() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap();
    let mut scheduler = scheduler.with_tick_interval(Duration::from_millis(1));
    let mut sleeper = TokioSleeper;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();

    assert_eq!(scheduler.state, SchedulerState::New);
    scheduler
        .run_until_stopped(&mut sleeper, shutdown_rx)
        .await
        .unwrap();

    assert_eq!(scheduler.state, SchedulerState::Closed);
    assert_eq!(scheduler.broker.close_calls, 1);
}

#[tokio::test]
async fn run_until_stopped_rejects_restart_after_shutdown() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap();
    let mut scheduler = scheduler.with_tick_interval(Duration::from_millis(1));
    let mut sleeper = TokioSleeper;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();
    scheduler
        .run_until_stopped(&mut sleeper, shutdown_rx)
        .await
        .unwrap();
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    let err = scheduler
        .run_until_stopped(&mut sleeper, shutdown_rx)
        .await
        .unwrap_err();

    assert_eq!(err, SchedulerError::AlreadyStopped);
}

#[tokio::test]
async fn run_method_matches_upstream_name() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap();

    let run = scheduler.run();

    std::mem::drop(run);
    assert_eq!(scheduler.state, SchedulerState::New);
}

#[tokio::test]
async fn run_until_stopped_rejects_duplicate_start() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap()
    .with_tick_interval(Duration::from_millis(1));
    let mut sleeper = TokioSleeper;
    scheduler.state = SchedulerState::Active;
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    let err = scheduler
        .run_until_stopped(&mut sleeper, shutdown_rx)
        .await
        .unwrap_err();

    assert_eq!(err, SchedulerError::AlreadyRunning);
    assert_eq!(scheduler.state, SchedulerState::Active);
}
