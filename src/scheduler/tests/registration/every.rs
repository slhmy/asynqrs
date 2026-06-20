use super::*;
use crate::scheduler::spec::duration::{parse_duration_spec, parse_go_duration_nanos};

#[test]
fn register_spec_accepts_every_duration_specs() {
    let now = SystemTime::UNIX_EPOCH;
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
            "@every 1h30m15s",
        )
        .unwrap();
    scheduler
        .register_spec(
            "fractional",
            Task::new("email:welcome", b"payload".to_vec()),
            "@every 1.5h",
        )
        .unwrap();
    scheduler
        .register_spec(
            "subsecond",
            Task::new("email:welcome", b"payload".to_vec()),
            "@every 1500ms",
        )
        .unwrap();
    scheduler
        .register_spec(
            "microsecond",
            Task::new("email:welcome", b"payload".to_vec()),
            "@every 250us",
        )
        .unwrap();

    assert_eq!(scheduler.entries.as_slice()[0].spec(), "@every 1h30m15s");
    assert_eq!(
        scheduler.entries.as_slice()[0].interval(),
        Duration::from_secs(5_415)
    );
    assert_eq!(
        scheduler.entries.as_slice()[0].next_enqueue_at(),
        now + Duration::from_secs(5_415)
    );
    assert_eq!(scheduler.entries.as_slice()[1].spec(), "@every 1.5h");
    assert_eq!(
        scheduler.entries.as_slice()[1].interval(),
        Duration::from_secs(5_400)
    );
    assert_eq!(scheduler.entries.as_slice()[2].spec(), "@every 1500ms");
    assert_eq!(
        scheduler.entries.as_slice()[2].interval(),
        Duration::from_secs(1)
    );
    assert_eq!(scheduler.entries.as_slice()[3].spec(), "@every 250us");
    assert_eq!(
        scheduler.entries.as_slice()[3].interval(),
        Duration::from_secs(1)
    );
}

#[test]
fn register_spec_limits_every_duration_to_go_duration_range() {
    assert_eq!(parse_duration_spec("0"), Ok(Duration::from_secs(1)));
    assert_eq!(parse_duration_spec("+0"), Ok(Duration::from_secs(1)));
    assert_eq!(parse_duration_spec("-0"), Ok(Duration::from_secs(1)));
    assert_eq!(
        parse_duration_spec("2562047h47m16.854775807s"),
        Ok(Duration::from_secs(9_223_372_036))
    );
    assert!(parse_duration_spec("2562047h47m16.854775808s").is_err());
    assert_eq!(
        parse_duration_spec("-2562047h47m16.854775808s"),
        Ok(Duration::from_secs(1))
    );
    assert!(parse_duration_spec("-2562047h47m16.854775809s").is_err());
}

#[test]
fn register_spec_parses_every_fraction_precision_like_go() {
    assert_eq!(parse_go_duration_nanos("0.9999999999999999999ns"), Ok(1));
    assert_eq!(
        parse_go_duration_nanos("0.123456789012345678901234567890s"),
        Ok(123_456_789)
    );
    assert_eq!(
        parse_go_duration_nanos("0.000000000000000000000000000001s"),
        Ok(0)
    );
}
