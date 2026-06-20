use super::*;

#[test]
fn encode_scheduler_enqueue_event_matches_upstream_metadata_wire_format() {
    let event =
        SchedulerEnqueueEventInfo::new("task-id".to_owned(), UNIX_EPOCH - Duration::from_nanos(1));

    let data = encode_scheduler_enqueue_event(Some(&event)).unwrap();
    let encoded = pb::asynq::SchedulerEnqueueEvent::decode(data.as_slice()).unwrap();

    assert_eq!(encoded.task_id, "task-id");
    assert_eq!(
        encoded.enqueue_time,
        Some(prost_types::Timestamp {
            seconds: -1,
            nanos: 999_999_999,
        })
    );
}

#[test]
fn encode_scheduler_enqueue_event_rejects_none_like_upstream_nil_event() {
    let error = encode_scheduler_enqueue_event(None).unwrap_err();

    assert_eq!(error.to_string(), "cannot encode nil enqueue event");
}

#[test]
fn decode_scheduler_enqueue_event_matches_upstream_metadata_wire_format() {
    let data = pb::asynq::SchedulerEnqueueEvent {
        task_id: "task-id".to_owned(),
        enqueue_time: Some(prost_types::Timestamp {
            seconds: 1_700_000_001,
            nanos: -1,
        }),
    }
    .encode_to_vec();

    let event = decode_scheduler_enqueue_event(&data).unwrap();

    assert_eq!(event.task_id(), "task-id");
    assert_eq!(
        event.enqueued_at(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_000) + Duration::from_nanos(999_999_999)
    );
}

#[test]
fn decode_scheduler_enqueue_event_reports_invalid_protobuf_like_upstream() {
    let error = decode_scheduler_enqueue_event(b"not protobuf").unwrap_err();

    assert!(
        error
            .to_string()
            .starts_with("invalid scheduler enqueue event: ")
    );
}

#[test]
fn encode_scheduler_entry_matches_upstream_metadata_wire_format() {
    let entry = SchedulerEntryInfo::new(
        "entry-id".to_owned(),
        "@every 1m".to_owned(),
        Task::new("email:welcome", b"payload".to_vec()),
        vec![
            "Queue(\"critical\")".to_owned(),
            "MaxRetry(5)".to_owned(),
            "malformed".to_owned(),
        ],
        UNIX_EPOCH + Duration::from_secs(1_700_000_000) + Duration::from_nanos(1),
        None,
    );

    let data = encode_scheduler_entry(Some(&entry)).unwrap();
    let encoded = pb::asynq::SchedulerEntry::decode(data.as_slice()).unwrap();

    assert_eq!(encoded.id, "entry-id");
    assert_eq!(encoded.spec, "@every 1m");
    assert_eq!(encoded.task_type, "email:welcome");
    assert_eq!(encoded.task_payload, b"payload");
    assert_eq!(
        encoded.enqueue_options,
        ["Queue(\"critical\")", "MaxRetry(5)", "malformed"]
    );
    assert_eq!(
        encoded.next_enqueue_time,
        Some(prost_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 1,
        })
    );
    assert_eq!(
        encoded.prev_enqueue_time,
        Some(prost_types::Timestamp {
            seconds: -62_135_596_800,
            nanos: 0,
        })
    );
}

#[test]
fn encode_scheduler_entry_rejects_none_like_upstream_nil_scheduler_entry() {
    let error = encode_scheduler_entry(None).unwrap_err();

    assert_eq!(error.to_string(), "cannot encode nil scheduler entry");
}

#[test]
fn decode_scheduler_entry_matches_upstream_metadata_wire_format() {
    let data = pb::asynq::SchedulerEntry {
        id: "entry-id".to_owned(),
        spec: "@every 1m".to_owned(),
        task_type: "email:welcome".to_owned(),
        task_payload: b"payload".to_vec(),
        enqueue_options: vec![
            "Queue(\"critical\")".to_owned(),
            "MaxRetry(5)".to_owned(),
            "malformed".to_owned(),
        ],
        next_enqueue_time: Some(prost_types::Timestamp {
            seconds: 1_700_000_061,
            nanos: -1,
        }),
        prev_enqueue_time: Some(prost_types::Timestamp {
            seconds: -62_135_596_800,
            nanos: 0,
        }),
    }
    .encode_to_vec();

    let entry = decode_scheduler_entry(&data).unwrap();

    assert_eq!(entry.id(), "entry-id");
    assert_eq!(entry.spec(), "@every 1m");
    assert_eq!(entry.task().task_type(), "email:welcome");
    assert_eq!(entry.task().payload(), b"payload");
    assert_eq!(
        entry.enqueue_options(),
        &["Queue(\"critical\")", "MaxRetry(5)", "malformed"]
    );
    assert_eq!(
        entry.next(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_060) + Duration::from_nanos(999_999_999)
    );
    assert_eq!(entry.prev(), None);
}

#[test]
fn decode_scheduler_entry_reports_invalid_protobuf_like_upstream() {
    let error = decode_scheduler_entry(b"not protobuf").unwrap_err();

    assert!(error.to_string().starts_with("invalid scheduler entry: "));
}
