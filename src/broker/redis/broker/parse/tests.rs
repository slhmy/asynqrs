use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prost::Message;

use super::decode_server_info;
use crate::pb;

fn system_time_from_unix_seconds_and_nanoseconds(seconds: i64, nanos: i32) -> SystemTime {
    // Reference: Asynq v0.26.0 protobuf timestamp decode uses
    // `Timestamp.AsTime`, which delegates to Go `time.Unix(sec, nsec)` and
    // normalizes nanoseconds together with seconds:
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

#[test]
fn server_info_decode_preserves_signed_queue_priorities() {
    let data = pb::asynq::ServerInfo {
        server_id: "server-id".to_owned(),
        host: "host".to_owned(),
        pid: 42,
        concurrency: i32::MIN,
        queues: HashMap::from([
            ("critical".to_owned(), i32::MIN),
            ("default".to_owned(), -1),
        ]),
        strict_priority: true,
        start_time: None,
        status: "active".to_owned(),
        active_worker_count: 0,
    }
    .encode_to_vec();

    let info = decode_server_info(data).unwrap();

    assert_eq!(info.concurrency(), i32::MIN);
    assert_eq!(info.queues().get("critical"), Some(&i32::MIN));
    assert_eq!(info.queues().get("default"), Some(&-1));
}

#[test]
fn protobuf_timestamp_decode_matches_go_unix_normalization() {
    assert_eq!(
        system_time_from_unix_seconds_and_nanoseconds(1_700_000_001, -1),
        UNIX_EPOCH + Duration::from_secs(1_700_000_000) + Duration::from_nanos(999_999_999)
    );
    assert_eq!(
        system_time_from_unix_seconds_and_nanoseconds(-1, 999_999_999),
        UNIX_EPOCH - Duration::from_nanos(1)
    );
    assert_eq!(
        system_time_from_unix_seconds_and_nanoseconds(10_000_000_000, 999_999_999),
        UNIX_EPOCH + Duration::from_secs(10_000_000_000) + Duration::from_nanos(999_999_999)
    );
}
