use super::*;

#[test]
fn redis_unix_seconds_match_go_time_unix_for_fractional_pre_epoch_times() {
    assert_eq!(unix_seconds_floor(UNIX_EPOCH - Duration::from_nanos(1)), -1);
    assert_eq!(
        unix_seconds_floor(UNIX_EPOCH - Duration::from_millis(500)),
        -1
    );
    assert_eq!(unix_seconds_floor(UNIX_EPOCH - Duration::from_secs(1)), -1);
    assert_eq!(
        unix_seconds_floor(UNIX_EPOCH + Duration::from_millis(500)),
        0
    );
}

#[test]
fn upstream_named_redis_plan_constants_match_rdb_defaults() {
    assert_eq!(DEFAULT_LEASE_DURATION, Duration::from_secs(30));
    assert_eq!(AGGREGATION_TIMEOUT, Duration::from_secs(2 * 60));
    assert_eq!(STATS_TTL, Duration::from_secs(90 * 24 * 60 * 60));
    assert_eq!(MAX_ARCHIVE_SIZE, 10_000);
    assert_eq!(ARCHIVED_EXPIRATION_DAYS, 90);
    assert_eq!(
        ARCHIVED_EXPIRATION,
        Duration::from_secs(ARCHIVED_EXPIRATION_DAYS as u64 * 24 * 60 * 60)
    );
}
