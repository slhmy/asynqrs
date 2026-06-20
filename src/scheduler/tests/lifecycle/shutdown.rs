use super::*;

#[tokio::test]
async fn shutdown_is_noop_for_new_scheduler() {
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap();

    scheduler.shutdown().await.unwrap();

    assert_eq!(scheduler.state, SchedulerState::New);
    assert_eq!(scheduler.broker.close_calls, 0);
    assert!(scheduler.broker.cleared.is_empty());
    assert!(scheduler.broker.cleared_history.is_empty());
}

#[tokio::test]
async fn shutdown_is_noop_for_closed_scheduler() {
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap();
    scheduler.state = SchedulerState::Closed;

    scheduler.shutdown().await.unwrap();

    assert_eq!(scheduler.state, SchedulerState::Closed);
    assert_eq!(scheduler.broker.close_calls, 0);
    assert!(scheduler.broker.cleared.is_empty());
    assert!(scheduler.broker.cleared_history.is_empty());
}

#[tokio::test]
async fn shutdown_clears_scheduler_entries_and_history_when_active() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap();
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
    assert_eq!(scheduler.broker.close_calls, 1);
    assert_eq!(scheduler.broker.cleared, ["scheduler-id"]);
    assert_eq!(scheduler.broker.cleared_history, ["entry-a", "entry-b"]);
}

#[tokio::test]
async fn shutdown_leaves_shared_scheduler_connection_open_like_upstream_from_redis_client() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap()
    .with_shared_connection();
    scheduler
        .register_spec(
            "entry-id",
            Task::new("email:welcome", b"payload".to_vec()),
            "@every 1m",
        )
        .unwrap();
    scheduler.begin_start().unwrap();

    scheduler.shutdown().await.unwrap();

    assert_eq!(scheduler.state, SchedulerState::Closed);
    assert_eq!(scheduler.broker.close_calls, 0);
    assert_eq!(scheduler.broker.cleared, ["scheduler-id"]);
    assert_eq!(scheduler.broker.cleared_history, ["entry-id"]);
}

#[tokio::test]
async fn shutdown_method_matches_upstream_name() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap();
    scheduler
        .register_spec(
            "entry-id",
            Task::new("email:welcome", b"payload".to_vec()),
            "@every 1m",
        )
        .unwrap();
    scheduler.begin_start().unwrap();

    scheduler.shutdown().await.unwrap();

    assert_eq!(scheduler.state, SchedulerState::Closed);
    assert_eq!(scheduler.broker.close_calls, 1);
    assert_eq!(scheduler.broker.cleared, ["scheduler-id"]);
    assert_eq!(scheduler.broker.cleared_history, ["entry-id"]);
}
