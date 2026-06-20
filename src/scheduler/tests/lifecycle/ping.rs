use super::*;

#[tokio::test]
async fn ping_delegates_to_broker_before_shutdown() {
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap();

    scheduler.ping().await.unwrap();

    assert_eq!(scheduler.broker.ping_calls, 1);
}

#[tokio::test]
async fn ping_returns_ok_after_scheduler_is_closed() {
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker {
            ping_error: Some("redis down".to_owned()),
            ..RecordingSchedulerBroker::default()
        },
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap();
    scheduler.state = SchedulerState::Closed;

    scheduler.ping().await.unwrap();

    assert_eq!(scheduler.broker.ping_calls, 0);
}

#[tokio::test]
async fn ping_reports_broker_errors_before_shutdown() {
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker {
            ping_error: Some("redis down".to_owned()),
            ..RecordingSchedulerBroker::default()
        },
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap();

    let err = scheduler.ping().await.unwrap_err();

    assert_eq!(err, SchedulerError::Ping("redis down".to_owned()));
    assert_eq!(err.to_string(), "redis down");
    assert_eq!(scheduler.broker.ping_calls, 1);
}

#[tokio::test]
async fn ping_method_matches_upstream_name() {
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap();

    scheduler.ping().await.unwrap();

    assert_eq!(scheduler.broker.ping_calls, 1);
}
