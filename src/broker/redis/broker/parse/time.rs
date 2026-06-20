use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(in crate::broker::redis::broker) fn system_time_from_unix_seconds(seconds: i64) -> SystemTime {
    if seconds >= 0 {
        UNIX_EPOCH + Duration::from_secs(seconds as u64)
    } else {
        UNIX_EPOCH - Duration::from_secs(seconds.unsigned_abs())
    }
}

pub(in crate::broker::redis::broker) fn system_time_from_unix_nanoseconds(
    nanoseconds: i64,
) -> SystemTime {
    if nanoseconds >= 0 {
        UNIX_EPOCH + Duration::from_nanos(nanoseconds as u64)
    } else {
        UNIX_EPOCH - Duration::from_nanos(nanoseconds.unsigned_abs())
    }
}

pub(in crate::broker::redis::broker) fn signed_duration_nanos(
    later: SystemTime,
    earlier: SystemTime,
) -> i128 {
    match later.duration_since(earlier) {
        Ok(duration) => duration_nanos_i128(duration),
        Err(error) => -duration_nanos_i128(error.duration()),
    }
}

fn duration_nanos_i128(duration: Duration) -> i128 {
    i128::from(duration.as_secs()) * 1_000_000_000 + i128::from(duration.subsec_nanos())
}
