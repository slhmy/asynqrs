use super::*;

#[test]
fn server_default_metadata_builder_uses_queue_selector() {
    let server = test_support::server_with_weighted_queues(
        recording_runtime(),
        [("critical", 6), ("default", 3)],
        RecordingSleeper::default(),
    )
    .unwrap()
    .with_default_server_metadata_for_workers(5)
    .unwrap();
    let metadata = server.metadata.as_ref().unwrap();
    let info = pb::asynq::ServerInfo::decode(metadata.server_info()).unwrap();

    assert_eq!(info.concurrency, 5);
    assert_eq!(info.queues.get("critical"), Some(&6));
    assert_eq!(info.queues.get("default"), Some(&3));
    assert!(!info.strict_priority);
}

#[test]
fn server_default_metadata_builder_uses_configured_worker_count() {
    let server = test_support::server_with_sleeper(
        recording_runtime(),
        ["critical"],
        RecordingSleeper::default(),
    )
    .unwrap()
    .with_worker_count(7)
    .with_default_server_metadata();
    let metadata = server.metadata.as_ref().unwrap();
    let info = pb::asynq::ServerInfo::decode(metadata.server_info()).unwrap();

    assert_eq!(info.concurrency, 7);
}
