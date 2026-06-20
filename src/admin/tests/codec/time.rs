use super::*;

#[test]
fn from_unix_time_or_zero_matches_upstream_zero_time_helper() {
    assert_eq!(from_unix_time_or_zero(0), None);
    assert_eq!(
        from_unix_time_or_zero(1_700_000_000),
        Some(UNIX_EPOCH + Duration::from_secs(1_700_000_000))
    );
    assert_eq!(
        from_unix_time_or_zero(-5),
        Some(UNIX_EPOCH - Duration::from_secs(5))
    );
}
