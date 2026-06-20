use super::*;

#[test]
fn scheduler_timestamp_normalizes_fractional_pre_epoch_times() {
    assert_eq!(
        timestamp(SystemTime::UNIX_EPOCH - Duration::from_nanos(1)),
        prost_types::Timestamp {
            seconds: -1,
            nanos: 999_999_999,
        }
    );
    assert_eq!(
        timestamp(SystemTime::UNIX_EPOCH - Duration::from_secs(1)),
        prost_types::Timestamp {
            seconds: -1,
            nanos: 0,
        }
    );
}

#[test]
fn scheduler_unix_seconds_match_go_for_fractional_pre_epoch_times() {
    assert_eq!(
        unix_seconds(SystemTime::UNIX_EPOCH - Duration::from_nanos(1)),
        -1
    );
    assert_eq!(
        unix_seconds(SystemTime::UNIX_EPOCH - Duration::from_millis(500)),
        -1
    );
    assert_eq!(
        unix_seconds(SystemTime::UNIX_EPOCH - Duration::from_secs(1)),
        -1
    );
    assert_eq!(
        unix_seconds(SystemTime::UNIX_EPOCH + Duration::from_millis(500)),
        0
    );
}
