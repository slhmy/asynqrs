use super::*;

#[test]
fn scheduler_log_level_builder_matches_supported_scheduler_options() {
    let scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap()
    .with_log_level(LogLevel::Warn);

    assert_eq!(scheduler.log_level(), LogLevel::Warn);
}

#[test]
fn scheduler_timezone_configures_cron_next_enqueue_times() {
    let now = Utc
        .with_ymd_and_hms(2023, 11, 14, 22, 13, 20)
        .unwrap()
        .into();
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap()
    .with_timezone(chrono_tz::Asia::Tokyo);

    scheduler
        .register_spec(
            "entry-id",
            Task::new("email:welcome", b"payload".to_vec()),
            "0 9 * * *",
        )
        .unwrap();

    let expected: SystemTime = Utc.with_ymd_and_hms(2023, 11, 15, 0, 0, 0).unwrap().into();
    assert_eq!(scheduler.timezone, chrono_tz::Asia::Tokyo);
    assert_eq!(scheduler.entries.as_slice()[0].next_enqueue_at(), expected);
}

#[test]
fn scheduler_heartbeat_interval_sets_default_ttl() {
    let scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap()
    .with_heartbeat_interval(Duration::from_secs(3));

    assert_eq!(scheduler.heartbeat_interval, Duration::from_secs(3));
    assert_eq!(scheduler.metadata_ttl, Duration::from_secs(6));
}
