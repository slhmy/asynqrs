use super::*;

#[test]
fn register_spec_accepts_cron_specs() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_030);
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
            "*/5 * * * *",
        )
        .unwrap();

    assert_eq!(scheduler.entries.as_slice()[0].spec(), "*/5 * * * *");
    assert!(scheduler.entries.as_slice()[0].is_cron());
    assert_eq!(scheduler.entries.as_slice()[0].interval(), Duration::ZERO);
    assert_eq!(
        scheduler.entries.as_slice()[0].next_enqueue_at(),
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_100)
    );

    let err = scheduler
        .register_spec(
            "seconds-field",
            Task::new("email:welcome", b"payload".to_vec()),
            "0 */5 * * * *",
        )
        .unwrap_err();
    assert_eq!(
        err,
        SchedulerError::UnsupportedSpec("0 */5 * * * *".to_owned())
    );

    scheduler
        .register_spec(
            "whitespace",
            Task::new("email:welcome", b"payload".to_vec()),
            "\t*/10   * * * *  ",
        )
        .unwrap();
    assert_eq!(scheduler.entries.as_slice()[1].spec(), "\t*/10   * * * *  ");
}

#[test]
fn register_spec_accepts_cron_timezone_prefixes() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap();

    scheduler
        .register_spec(
            "cron-tz",
            Task::new("email:welcome", b"payload".to_vec()),
            "CRON_TZ=Asia/Tokyo 0 9 * * *",
        )
        .unwrap();
    scheduler
        .register_spec(
            "tz",
            Task::new("email:welcome", b"payload".to_vec()),
            "TZ=America/New_York 0 9 * * *",
        )
        .unwrap();

    assert_eq!(
        scheduler.entries.as_slice()[0].spec(),
        "CRON_TZ=Asia/Tokyo 0 9 * * *"
    );
    assert_eq!(
        scheduler.entries.as_slice()[0].next_enqueue_at(),
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_006_400)
    );
    assert_eq!(
        scheduler.entries.as_slice()[1].spec(),
        "TZ=America/New_York 0 9 * * *"
    );
    assert_eq!(
        scheduler.entries.as_slice()[1].next_enqueue_at(),
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_056_800)
    );
}

#[test]
fn register_spec_treats_question_mark_as_cron_wildcard_like_upstream() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap();

    scheduler
        .register_spec(
            "minute-wildcard",
            Task::new("email:welcome", b"payload".to_vec()),
            "? * * * *",
        )
        .unwrap();
    scheduler
        .register_spec(
            "dow-wildcard",
            Task::new("email:welcome", b"payload".to_vec()),
            "0 12 * * ?",
        )
        .unwrap();

    assert_eq!(scheduler.entries.as_slice()[0].spec(), "? * * * *");
    assert_eq!(
        scheduler.entries.as_slice()[0].next_enqueue_at(),
        SystemTime::UNIX_EPOCH + Duration::from_secs(60)
    );
    assert_eq!(scheduler.entries.as_slice()[1].spec(), "0 12 * * ?");
    assert!(scheduler.entries.as_slice()[1].is_cron());
}

#[test]
fn register_spec_treats_question_mark_step_as_cron_wildcard_step_like_upstream() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(6 * 60);
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap();

    scheduler
        .register_spec(
            "minute-step",
            Task::new("email:welcome", b"payload".to_vec()),
            "?/15 * * * *",
        )
        .unwrap();
    scheduler
        .register_spec(
            "minute-range",
            Task::new("email:welcome", b"payload".to_vec()),
            "?-5 * * * *",
        )
        .unwrap();
    scheduler
        .register_spec(
            "minute-range-step",
            Task::new("email:welcome", b"payload".to_vec()),
            "?-5/2 * * * *",
        )
        .unwrap();

    assert_eq!(scheduler.entries.as_slice()[0].spec(), "?/15 * * * *");
    assert_eq!(
        scheduler.entries.as_slice()[0].next_enqueue_at(),
        SystemTime::UNIX_EPOCH + Duration::from_secs(900)
    );
    assert_eq!(scheduler.entries.as_slice()[1].spec(), "?-5 * * * *");
    assert_eq!(
        scheduler.entries.as_slice()[1].next_enqueue_at(),
        SystemTime::UNIX_EPOCH + Duration::from_secs(7 * 60)
    );
    assert_eq!(scheduler.entries.as_slice()[2].spec(), "?-5/2 * * * *");
    assert_eq!(
        scheduler.entries.as_slice()[2].next_enqueue_at(),
        SystemTime::UNIX_EPOCH + Duration::from_secs(8 * 60)
    );
}

#[test]
fn register_spec_treats_star_range_as_full_cron_wildcard_like_upstream() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(6 * 60);
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap();

    scheduler
        .register_spec(
            "minute-star-range",
            Task::new("email:welcome", b"payload".to_vec()),
            "*-5 * * * *",
        )
        .unwrap();
    scheduler
        .register_spec(
            "minute-star-range-step",
            Task::new("email:welcome", b"payload".to_vec()),
            "*-5/2 * * * *",
        )
        .unwrap();

    assert_eq!(scheduler.entries.as_slice()[0].spec(), "*-5 * * * *");
    assert_eq!(
        scheduler.entries.as_slice()[0].next_enqueue_at(),
        SystemTime::UNIX_EPOCH + Duration::from_secs(7 * 60)
    );
    assert_eq!(scheduler.entries.as_slice()[1].spec(), "*-5/2 * * * *");
    assert_eq!(
        scheduler.entries.as_slice()[1].next_enqueue_at(),
        SystemTime::UNIX_EPOCH + Duration::from_secs(8 * 60)
    );
}

#[test]
fn register_spec_accepts_cron_descriptors() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_030);
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap();

    scheduler
        .register_spec(
            "hourly",
            Task::new("email:welcome", b"payload".to_vec()),
            "@hourly",
        )
        .unwrap();
    scheduler
        .register_spec(
            "midnight",
            Task::new("email:welcome", b"payload".to_vec()),
            "@midnight",
        )
        .unwrap();
    scheduler
        .register_spec(
            "annually",
            Task::new("email:welcome", b"payload".to_vec()),
            "@annually",
        )
        .unwrap();

    assert_eq!(scheduler.entries.as_slice()[0].spec(), "@hourly");
    assert_eq!(
        scheduler.entries.as_slice()[0].next_enqueue_at(),
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_002_800)
    );
    assert_eq!(scheduler.entries.as_slice()[1].spec(), "@midnight");
    assert_eq!(
        scheduler.entries.as_slice()[1].next_enqueue_at(),
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_006_400)
    );
    assert_eq!(scheduler.entries.as_slice()[2].spec(), "@annually");
    assert_eq!(
        scheduler.entries.as_slice()[2].next_enqueue_at(),
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_704_067_200)
    );
}

#[test]
fn register_spec_rejects_unsupported_specs() {
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap();

    let err = scheduler
        .register_spec(
            "entry-id",
            Task::new("email:welcome", b"payload".to_vec()),
            "not-a-cron-spec",
        )
        .unwrap_err();

    assert_eq!(
        err,
        SchedulerError::UnsupportedSpec("not-a-cron-spec".to_owned())
    );

    let err = scheduler
        .register_spec(
            "entry-id",
            Task::new("email:welcome", b"payload".to_vec()),
            "@every1h",
        )
        .unwrap_err();

    assert_eq!(err, SchedulerError::UnsupportedSpec("@every1h".to_owned()));

    let err = scheduler
        .register_spec(
            "entry-id",
            Task::new("email:welcome", b"payload".to_vec()),
            "CRON_TZ=No/SuchZone 0 9 * * *",
        )
        .unwrap_err();

    assert_eq!(
        err,
        SchedulerError::UnsupportedSpec("CRON_TZ=No/SuchZone 0 9 * * *".to_owned())
    );

    let err = scheduler
        .register_spec(
            "entry-id",
            Task::new("email:welcome", b"payload".to_vec()),
            " @hourly",
        )
        .unwrap_err();

    assert_eq!(err, SchedulerError::UnsupportedSpec(" @hourly".to_owned()));

    let err = scheduler
        .register_spec(
            "entry-id",
            Task::new("email:welcome", b"payload".to_vec()),
            "@hourly ",
        )
        .unwrap_err();

    assert_eq!(err, SchedulerError::UnsupportedSpec("@hourly ".to_owned()));

    let err = scheduler
        .register_spec(
            "entry-id",
            Task::new("email:welcome", b"payload".to_vec()),
            " @every 1h",
        )
        .unwrap_err();

    assert_eq!(
        err,
        SchedulerError::UnsupportedSpec(" @every 1h".to_owned())
    );

    let err = scheduler
        .register_spec(
            "entry-id",
            Task::new("email:welcome", b"payload".to_vec()),
            "@every 1h ",
        )
        .unwrap_err();

    assert_eq!(
        err,
        SchedulerError::UnsupportedSpec("@every 1h ".to_owned())
    );

    let err = scheduler
        .register_spec(
            "entry-id",
            Task::new("email:welcome", b"payload".to_vec()),
            "TZ=UTC\t0 9 * * *",
        )
        .unwrap_err();

    assert_eq!(
        err,
        SchedulerError::UnsupportedSpec("TZ=UTC\t0 9 * * *".to_owned())
    );

    let err = scheduler
        .register_spec(
            "entry-id",
            Task::new("email:welcome", b"payload".to_vec()),
            " CRON_TZ=UTC 0 9 * * *",
        )
        .unwrap_err();

    assert_eq!(
        err,
        SchedulerError::UnsupportedSpec(" CRON_TZ=UTC 0 9 * * *".to_owned())
    );
}
