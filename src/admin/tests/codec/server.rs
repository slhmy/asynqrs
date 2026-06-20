use super::*;

#[test]
fn decode_server_info_matches_upstream_metadata_wire_format() {
    let data = pb::asynq::ServerInfo {
        server_id: "server-id".to_owned(),
        host: "host.local".to_owned(),
        pid: 1234,
        concurrency: i32::MIN,
        queues: HashMap::from([
            ("critical".to_owned(), i32::MIN),
            ("default".to_owned(), -1),
        ]),
        strict_priority: true,
        start_time: Some(prost_types::Timestamp {
            seconds: 1_700_000_001,
            nanos: -1,
        }),
        status: "active".to_owned(),
        active_worker_count: 0,
    }
    .encode_to_vec();

    let info = decode_server_info(&data).unwrap();

    assert_eq!(info.id(), "server-id");
    assert_eq!(info.host(), "host.local");
    assert_eq!(info.pid(), 1234);
    assert_eq!(info.concurrency(), i32::MIN);
    assert_eq!(info.queues().get("critical"), Some(&i32::MIN));
    assert_eq!(info.queues().get("default"), Some(&-1));
    assert!(info.strict_priority());
    assert_eq!(
        info.started(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_000) + Duration::from_nanos(999_999_999)
    );
    assert_eq!(info.status(), "active");
    assert!(info.active_workers().is_empty());
}

#[test]
fn decode_server_info_reports_invalid_protobuf_like_upstream() {
    let error = decode_server_info(b"not protobuf").unwrap_err();

    assert!(error.to_string().starts_with("invalid server info: "));
}

#[test]
fn encode_server_info_matches_upstream_metadata_wire_format() {
    let workers = vec![
        WorkerInfo::new(
            "worker-a".to_owned(),
            "email:welcome".to_owned(),
            Vec::new(),
            "critical".to_owned(),
            UNIX_EPOCH,
            UNIX_EPOCH,
        ),
        WorkerInfo::new(
            "worker-b".to_owned(),
            "email:receipt".to_owned(),
            Vec::new(),
            "default".to_owned(),
            UNIX_EPOCH,
            UNIX_EPOCH,
        ),
    ];
    let info = ServerInfo::new(ServerInfoParts {
        id: "server-id".to_owned(),
        host: "host.local".to_owned(),
        pid: i32::MIN,
        concurrency: i32::MIN,
        queues: HashMap::from([
            ("critical".to_owned(), i32::MIN),
            ("default".to_owned(), -1),
        ]),
        strict_priority: true,
        started: UNIX_EPOCH - Duration::from_nanos(1),
        status: "active".to_owned(),
        active_workers: workers,
    });

    let data = encode_server_info(Some(&info)).unwrap();
    let encoded = pb::asynq::ServerInfo::decode(data.as_slice()).unwrap();

    assert_eq!(encoded.host, "host.local");
    assert_eq!(encoded.pid, i32::MIN);
    assert_eq!(encoded.server_id, "server-id");
    assert_eq!(encoded.concurrency, i32::MIN);
    assert_eq!(encoded.queues.get("critical"), Some(&i32::MIN));
    assert_eq!(encoded.queues.get("default"), Some(&-1));
    assert!(encoded.strict_priority);
    assert_eq!(encoded.status, "active");
    assert_eq!(
        encoded.start_time,
        Some(prost_types::Timestamp {
            seconds: -1,
            nanos: 999_999_999,
        })
    );
    assert_eq!(encoded.active_worker_count, 2);
}

#[test]
fn encode_server_info_rejects_none_like_upstream_nil_server_info() {
    let error = encode_server_info(None).unwrap_err();

    assert_eq!(error.to_string(), "cannot encode nil server info");
}
