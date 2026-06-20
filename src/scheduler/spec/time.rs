use std::time::SystemTime;

use chrono::{DateTime, Utc};

use crate::SchedulerError;

pub(in crate::scheduler) fn unix_seconds(time: SystemTime) -> i64 {
    // Reference: Go `time.Time.Unix` returns the local time as Unix seconds,
    // including fractional pre-epoch values rounded down to the previous
    // second:
    // <https://go.dev/src/time/time.go>.
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => duration.as_secs() as i64,
        Err(error) => {
            let duration = error.duration();
            let seconds = duration.as_secs() as i64;
            if duration.subsec_nanos() == 0 {
                -seconds
            } else {
                -seconds.saturating_add(1)
            }
        }
    }
}

pub(super) fn system_time_to_utc(time: SystemTime) -> Result<DateTime<Utc>, SchedulerError> {
    Ok(DateTime::<Utc>::from(time))
}
