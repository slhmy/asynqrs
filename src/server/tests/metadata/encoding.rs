use super::*;
use crate::server::metadata::codec::server_pid_i32;

fn timestamp(time: SystemTime) -> prost_types::Timestamp {
    // Reference: Asynq v0.26.0 encodes heartbeat timestamps with
    // `timestamppb.New`, which normalizes fractional pre-epoch times to
    // non-negative nanos:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L349-L365>.
    match time.duration_since(SystemTime::UNIX_EPOCH) {
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

#[test]
fn default_server_metadata_encodes_server_info_proto() {
    let queues = vec!["critical".to_owned(), "default".to_owned()];

    let metadata = ServerMetadata::for_current_process(&queues);
    let info = pb::asynq::ServerInfo::decode(metadata.server_info()).unwrap();

    assert_eq!(info.host, metadata.hostname());
    assert_eq!(info.pid, metadata.pid());
    assert_eq!(info.server_id, metadata.server_id());
    assert_eq!(info.concurrency, 1);
    assert_eq!(info.queues.get("critical"), Some(&1));
    assert_eq!(info.queues.get("default"), Some(&1));
    assert!(!info.strict_priority);
    assert_eq!(info.status, "active");
    assert!(info.start_time.is_some());
    assert_eq!(info.active_worker_count, 0);
    assert_eq!(metadata.ttl(), DEFAULT_SERVER_METADATA_TTL);
}

#[test]
fn server_metadata_hostname_falls_back_like_upstream() {
    assert_eq!(
        normalize_hostname(Some("worker-host".to_owned())),
        "worker-host"
    );
    assert_eq!(normalize_hostname(Some(" ".to_owned())), "unknown-host");
    assert_eq!(normalize_hostname(None), "unknown-host");
}

#[test]
fn server_timestamp_normalizes_fractional_pre_epoch_times() {
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
fn default_server_metadata_has_no_idle_worker_info_protos() {
    let queues = vec!["critical".to_owned(), "default".to_owned()];

    let metadata = ServerMetadata::for_current_process(&queues);

    assert!(metadata.workers().is_empty());
}

#[test]
fn worker_info_bytes_uses_upstream_encode_worker_info_zero_deadline() {
    let mut message = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    message.id = "task-id".to_owned();
    message.queue = "critical".to_owned();
    let encoded = worker_info_bytes(
        "host.local",
        i32::MIN,
        "server-id",
        &message,
        UNIX_EPOCH + Duration::from_secs(1_700_000_000),
        None,
    );

    let worker = pb::asynq::WorkerInfo::decode(encoded.as_slice()).unwrap();

    assert_eq!(worker.host, "host.local");
    assert_eq!(worker.pid, i32::MIN);
    assert_eq!(worker.server_id, "server-id");
    assert_eq!(worker.task_id, "task-id");
    assert_eq!(worker.task_type, "email:welcome");
    assert_eq!(worker.task_payload, b"payload");
    assert_eq!(worker.queue, "critical");
    assert_eq!(
        worker.start_time,
        Some(prost_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        })
    );
    assert_eq!(
        worker.deadline,
        Some(prost_types::Timestamp {
            seconds: -62_135_596_800,
            nanos: 0,
        })
    );
}

#[test]
fn default_server_metadata_preserves_queue_selector_info() {
    let selector = QueueSelector::weighted_priority([("critical", 10), ("default", 5)]).unwrap();

    let metadata = ServerMetadata::for_current_process_with_queue_selector(&selector, 8).unwrap();
    let info = pb::asynq::ServerInfo::decode(metadata.server_info()).unwrap();

    assert!(metadata.workers().is_empty());
    assert_eq!(info.concurrency, 8);
    assert_eq!(info.queues.get("critical"), Some(&10));
    assert_eq!(info.queues.get("default"), Some(&5));
    assert!(!info.strict_priority);
}

#[test]
fn default_server_metadata_uses_upstream_int32_conversions() {
    let selector =
        QueueSelector::weighted_priority([("critical", 2_147_483_648usize), ("default", 1)])
            .unwrap();

    let metadata =
        ServerMetadata::for_current_process_with_queue_selector(&selector, 2_147_483_648).unwrap();
    let info = pb::asynq::ServerInfo::decode(metadata.server_info()).unwrap();

    assert_eq!(info.concurrency, i32::MIN);
    assert_eq!(info.queues.get("critical"), Some(&i32::MIN));
    assert_eq!(server_info_i32(2_147_483_648), i32::MIN);
}

#[test]
fn default_server_metadata_uses_upstream_pid_int32_conversion() {
    assert_eq!(server_pid_i32(2_147_483_647), i32::MAX);
    assert_eq!(server_pid_i32(2_147_483_648), i32::MIN);
    assert_eq!(server_pid_i32(u32::MAX), -1);
}

#[test]
fn default_server_metadata_marks_strict_priority_queues() {
    let selector =
        QueueSelector::strict_priority([("low", 1), ("critical", 6), ("default", 3)]).unwrap();

    let metadata = ServerMetadata::for_current_process_with_queue_selector(&selector, 4).unwrap();
    let info = pb::asynq::ServerInfo::decode(metadata.server_info()).unwrap();

    assert!(metadata.workers().is_empty());
    assert_eq!(info.concurrency, 4);
    assert_eq!(info.queues.get("critical"), Some(&6));
    assert_eq!(info.queues.get("default"), Some(&3));
    assert_eq!(info.queues.get("low"), Some(&1));
    assert!(info.strict_priority);
}
