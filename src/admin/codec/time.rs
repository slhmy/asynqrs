use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Converts task-message Unix seconds into Rust's zero-time representation.
///
/// Reference: Asynq v0.26.0 `from_unix_time_or_zero`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L157-L162>.
///
/// Rust compatibility: `Option<SystemTime>` is the explicit zero-time
/// representation for public task metadata.
pub fn from_unix_time_or_zero(seconds: i64) -> Option<SystemTime> {
    (seconds != 0).then(|| system_time_from_unix_seconds(seconds))
}

pub(super) fn optional_system_time_from_protobuf_timestamp(
    timestamp: Option<prost_types::Timestamp>,
) -> SystemTime {
    timestamp
        .map(|timestamp| {
            system_time_from_unix_seconds_and_nanoseconds(timestamp.seconds, timestamp.nanos)
        })
        .unwrap_or(UNIX_EPOCH)
}

pub(super) fn system_time_from_protobuf_timestamp(
    timestamp: Option<prost_types::Timestamp>,
) -> SystemTime {
    // Reference: Asynq v0.26.0 protobuf timestamp decode uses
    // `Get...().AsTime()`, so a missing timestamp decodes as Unix epoch.
    // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L570-L581>.
    optional_system_time_from_protobuf_timestamp(timestamp)
}

pub(super) fn optional_scheduler_prev_time(
    timestamp: Option<prost_types::Timestamp>,
) -> Option<SystemTime> {
    match timestamp {
        // Go's zero time is encoded by timestamppb.New(time.Time{}).
        Some(timestamp) if timestamp.seconds == -62_135_596_800 && timestamp.nanos == 0 => None,
        timestamp => Some(system_time_from_protobuf_timestamp(timestamp)),
    }
}

pub(super) fn go_zero_time_timestamp() -> prost_types::Timestamp {
    // Reference: Asynq v0.26.0 metadata encoders use `timestamppb.New`, so
    // unset scheduler entry previous enqueue times are encoded from Go's zero
    // `time.Time{}` value, 0001-01-01T00:00:00Z.
    // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L527-L545>.
    prost_types::Timestamp {
        seconds: -62_135_596_800,
        nanos: 0,
    }
}

pub(super) fn system_time_to_protobuf_timestamp(time: SystemTime) -> prost_types::Timestamp {
    // Reference: Asynq v0.26.0 metadata encoders use `timestamppb.New`, which
    // normalizes fractional pre-epoch times to non-negative nanos.
    // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go>.
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => prost_types::Timestamp {
            seconds: duration.as_secs().try_into().unwrap_or(i64::MAX),
            nanos: duration.subsec_nanos() as i32,
        },
        Err(error) => {
            let duration = error.duration();
            let seconds = i64::try_from(duration.as_secs()).unwrap_or(i64::MAX);
            let nanos = duration.subsec_nanos() as i32;
            if nanos == 0 {
                prost_types::Timestamp {
                    seconds: seconds.saturating_neg(),
                    nanos: 0,
                }
            } else {
                prost_types::Timestamp {
                    seconds: seconds.saturating_add(1).saturating_neg(),
                    nanos: 1_000_000_000 - nanos,
                }
            }
        }
    }
}

fn system_time_from_unix_seconds(seconds: i64) -> SystemTime {
    if seconds >= 0 {
        UNIX_EPOCH + Duration::from_secs(seconds as u64)
    } else {
        UNIX_EPOCH - Duration::from_secs(seconds.unsigned_abs())
    }
}

fn system_time_from_unix_seconds_and_nanoseconds(seconds: i64, nanos: i32) -> SystemTime {
    // Reference: Asynq v0.26.0 `decode_server_info` uses
    // `timestamppb.Timestamp.AsTime`, which normalizes nanoseconds like
    // Go `time.Unix(sec, nsec)`.
    // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L402-L424>.
    // <https://pkg.go.dev/google.golang.org/protobuf/types/known/timestamppb#Timestamp.AsTime>.
    let total_nanos = i128::from(seconds)
        .saturating_mul(1_000_000_000)
        .saturating_add(i128::from(nanos));
    let normalized_seconds = total_nanos.div_euclid(1_000_000_000);
    let normalized_nanos = total_nanos.rem_euclid(1_000_000_000) as u32;
    let whole_seconds = Duration::from_secs(normalized_seconds.unsigned_abs() as u64);
    let fractional = Duration::from_nanos(u64::from(normalized_nanos));
    if normalized_seconds >= 0 {
        UNIX_EPOCH + whole_seconds + fractional
    } else if normalized_nanos == 0 {
        UNIX_EPOCH - whole_seconds
    } else {
        UNIX_EPOCH - whole_seconds + fractional
    }
}
