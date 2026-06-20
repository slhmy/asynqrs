use super::*;

#[test]
fn encode_worker_info_matches_upstream_metadata_wire_format() {
    let info = WorkerMetadataInfo::new(WorkerMetadataInfoParts {
        host: "host.local".to_owned(),
        pid: i32::MIN,
        server_id: "server-id".to_owned(),
        task_id: "task-id".to_owned(),
        task_type: "email:welcome".to_owned(),
        task_payload: b"payload".to_vec(),
        queue: "critical".to_owned(),
        started: UNIX_EPOCH + Duration::from_secs(1_700_000_000) + Duration::from_nanos(1),
        deadline: UNIX_EPOCH - Duration::from_nanos(1),
    });

    let data = encode_worker_info(Some(&info)).unwrap();
    let encoded = pb::asynq::WorkerInfo::decode(data.as_slice()).unwrap();

    assert_eq!(encoded.host, "host.local");
    assert_eq!(encoded.pid, i32::MIN);
    assert_eq!(encoded.server_id, "server-id");
    assert_eq!(encoded.task_id, "task-id");
    assert_eq!(encoded.task_type, "email:welcome");
    assert_eq!(encoded.task_payload, b"payload");
    assert_eq!(encoded.queue, "critical");
    assert_eq!(
        encoded.start_time,
        Some(prost_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 1,
        })
    );
    assert_eq!(
        encoded.deadline,
        Some(prost_types::Timestamp {
            seconds: -1,
            nanos: 999_999_999,
        })
    );
}

#[test]
fn encode_worker_info_rejects_none_like_upstream_nil_worker_info() {
    let error = encode_worker_info(None).unwrap_err();

    assert_eq!(error.to_string(), "cannot encode nil worker info");
}

#[test]
fn decode_worker_info_matches_upstream_metadata_wire_format() {
    let data = pb::asynq::WorkerInfo {
        host: "host.local".to_owned(),
        pid: i32::MIN,
        server_id: "server-id".to_owned(),
        task_id: "task-id".to_owned(),
        task_type: "email:welcome".to_owned(),
        task_payload: b"payload".to_vec(),
        queue: "critical".to_owned(),
        start_time: Some(prost_types::Timestamp {
            seconds: 1_700_000_001,
            nanos: -1,
        }),
        deadline: Some(prost_types::Timestamp {
            seconds: -1,
            nanos: 999_999_999,
        }),
    }
    .encode_to_vec();

    let started =
        UNIX_EPOCH + Duration::from_secs(1_700_000_000) + Duration::from_nanos(999_999_999);
    let deadline = UNIX_EPOCH - Duration::from_nanos(1);
    let next_started = started + Duration::from_secs(30);
    let next_deadline = deadline + Duration::from_secs(30);
    let mut info = decode_worker_info(&data).unwrap();

    info.host_mut().push_str(".cluster");
    info.host_mut().push_str(".local");
    *info.pid_mut() += 1;
    *info.pid_mut() += 2;
    info.server_id_mut().push_str("-a");
    info.server_id_mut().push_str("-b");
    info.task_id_mut().push_str("-a");
    info.task_id_mut().push_str("-b");
    info.task_type_mut().push_str(":v1");
    info.task_type_mut().push_str(":v2");
    info.task_payload_mut().extend_from_slice(b"-a");
    info.task_payload_mut().extend_from_slice(b"-b");
    info.queue_mut().push_str("-tenant");
    info.queue_mut().push_str("-a");
    *info.started_mut() = next_started;
    *info.started_mut() = started;
    *info.deadline_mut() = next_deadline;
    *info.deadline_mut() = deadline;

    assert_eq!(info.host(), "host.local.cluster.local");
    assert_eq!(info.host(), "host.local.cluster.local");
    assert_eq!(info.pid(), i32::MIN + 3);
    assert_eq!(info.pid(), i32::MIN + 3);
    assert_eq!(info.server_id(), "server-id-a-b");
    assert_eq!(info.server_id(), "server-id-a-b");
    assert_eq!(info.task_id(), "task-id-a-b");
    assert_eq!(info.task_id(), "task-id-a-b");
    assert_eq!(info.task_type(), "email:welcome:v1:v2");
    assert_eq!(info.task_type(), "email:welcome:v1:v2");
    assert_eq!(info.task_payload(), b"payload-a-b");
    assert_eq!(info.task_payload(), b"payload-a-b");
    assert_eq!(info.queue(), "critical-tenant-a");
    assert_eq!(info.queue(), "critical-tenant-a");
    assert_eq!(info.started(), started);
    assert_eq!(info.started(), started);
    assert_eq!(info.deadline(), deadline);
    assert_eq!(info.deadline(), deadline);
}

#[test]
fn decode_worker_info_reports_invalid_protobuf_like_upstream() {
    let error = decode_worker_info(b"not protobuf").unwrap_err();

    assert!(error.to_string().starts_with("invalid worker info: "));
}
