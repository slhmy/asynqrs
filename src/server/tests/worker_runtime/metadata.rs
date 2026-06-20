use super::*;
use prost::Message;

#[test]
fn metadata_snapshot_serializes_active_worker() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let deadline = now + Duration::from_secs(30);
    let mut active = message("task-id");
    active.timeout = 30;
    let mut assembly = WorkerAssembly::with_parts(
        RecordingBroker::default(),
        |_task: &Task| Ok::<(), HandlerError>(()),
        DefaultRetryDelay,
        TestClock(now),
    );
    assembly.start_active_worker(
        &active,
        now,
        Some(deadline),
        ProcessingLease::new(now + Duration::from_secs(30)),
    );
    let metadata = ServerMetadata::new(
        "host",
        123,
        "server-id",
        crate::pb::asynq::ServerInfo {
            host: "host".to_owned(),
            pid: 123,
            server_id: "server-id".to_owned(),
            concurrency: 1,
            queues: [("critical".to_owned(), 1)].into_iter().collect(),
            strict_priority: false,
            status: "active".to_owned(),
            start_time: None,
            active_worker_count: 0,
        }
        .encode_to_vec(),
        std::iter::empty::<String>(),
        Duration::from_secs(30),
    )
    .unwrap();

    let snapshot = assembly.metadata_snapshot(&metadata);
    let server_info = crate::pb::asynq::ServerInfo::decode(snapshot.server_info()).unwrap();
    let worker = crate::pb::asynq::WorkerInfo::decode(snapshot.workers()[0].as_slice()).unwrap();

    assert_eq!(server_info.active_worker_count, 1);
    assert_eq!(snapshot.workers().len(), 1);
    assert_eq!(worker.host, "host");
    assert_eq!(worker.pid, 123);
    assert_eq!(worker.server_id, "server-id");
    assert_eq!(worker.task_id, "task-id");
    assert_eq!(worker.task_type, "email:welcome");
    assert_eq!(worker.task_payload, b"payload");
    assert_eq!(worker.queue, "critical");
    assert_eq!(
        worker.start_time,
        Some(prost_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 0
        })
    );
    assert_eq!(
        worker.deadline,
        Some(prost_types::Timestamp {
            seconds: 1_700_000_030,
            nanos: 0
        })
    );
}

#[test]
fn assembly_runtime_accessor_exposes_server_owned_active_worker_snapshot() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut active = message("task-id");
    active.timeout = 30;
    let mut assembly = WorkerAssembly::with_parts(
        RecordingBroker::default(),
        |_task: &Task| Ok::<(), HandlerError>(()),
        DefaultRetryDelay,
        TestClock(now),
    );
    assembly.start_active_worker(
        &active,
        now,
        Some(now + Duration::from_secs(30)),
        ProcessingLease::new(now + Duration::from_secs(30)),
    );
    let metadata = ServerMetadata::new(
        "host",
        123,
        "server-id",
        crate::pb::asynq::ServerInfo {
            host: "host".to_owned(),
            pid: 123,
            server_id: "server-id".to_owned(),
            concurrency: 1,
            queues: [("critical".to_owned(), 1)].into_iter().collect(),
            strict_priority: false,
            status: "active".to_owned(),
            start_time: None,
            active_worker_count: 0,
        }
        .encode_to_vec(),
        std::iter::empty::<String>(),
        Duration::from_secs(30),
    )
    .unwrap();

    let runtime = assembly.runtime_state();
    let snapshot = runtime.server_metadata_snapshot(&metadata);
    let server_info = crate::pb::asynq::ServerInfo::decode(snapshot.server_info()).unwrap();
    let worker = crate::pb::asynq::WorkerInfo::decode(snapshot.workers()[0].as_slice()).unwrap();

    assert_eq!(server_info.active_worker_count, 1);
    assert_eq!(worker.task_id, "task-id");
    assert_eq!(worker.queue, "critical");
}

#[test]
fn metadata_snapshot_aggregates_active_workers_from_assembly_clones() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut first = message("task-a");
    first.timeout = 30;
    let mut second = message("task-b");
    second.timeout = 60;
    let mut assembly = WorkerAssembly::with_parts(
        RecordingBroker::default(),
        |_task: &Task| Ok::<(), HandlerError>(()),
        DefaultRetryDelay,
        TestClock(now),
    );
    let mut clone = WorkerAssembly::with_parts(
        RecordingBroker::default(),
        |_task: &Task| Ok::<(), HandlerError>(()),
        DefaultRetryDelay,
        TestClock(now),
    );
    clone.runtime = assembly.runtime.clone();
    assembly.start_active_worker(
        &second,
        now + Duration::from_secs(1),
        None,
        ProcessingLease::new(now + Duration::from_secs(30)),
    );
    clone.start_active_worker(
        &first,
        now,
        Some(now + Duration::from_secs(30)),
        ProcessingLease::new(now + Duration::from_secs(30)),
    );
    let metadata = ServerMetadata::new(
        "host",
        123,
        "server-id",
        crate::pb::asynq::ServerInfo {
            host: "host".to_owned(),
            pid: 123,
            server_id: "server-id".to_owned(),
            concurrency: 2,
            queues: [("critical".to_owned(), 1)].into_iter().collect(),
            strict_priority: false,
            status: "active".to_owned(),
            start_time: None,
            active_worker_count: 0,
        }
        .encode_to_vec(),
        std::iter::empty::<String>(),
        Duration::from_secs(30),
    )
    .unwrap();

    let snapshot = assembly.metadata_snapshot(&metadata);
    let server_info = crate::pb::asynq::ServerInfo::decode(snapshot.server_info()).unwrap();
    let workers: Vec<_> = snapshot
        .workers()
        .iter()
        .map(|worker| crate::pb::asynq::WorkerInfo::decode(worker.as_slice()).unwrap())
        .collect();

    assert_eq!(server_info.active_worker_count, 2);
    assert_eq!(workers[0].task_id, "task-a");
    assert_eq!(workers[1].task_id, "task-b");
}

#[tokio::test]
async fn heartbeat_extends_shared_active_worker_leases_like_upstream() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let active = message("task-id");
    let lease = ProcessingLease::new(now + Duration::from_secs(5));
    let mut assembly = WorkerAssembly::with_parts(
        RecordingBroker::default(),
        |_task: &Task| Ok::<(), HandlerError>(()),
        DefaultRetryDelay,
        TestClock(now),
    );
    assembly.start_active_worker(&active, now, None, lease.clone());

    assembly.extend_active_worker_leases().await;

    assert_eq!(
        assembly.broker.lease_extensions,
        [("critical".to_owned(), "task-id".to_owned())]
    );
    assert_eq!(
        lease.expires_at(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_030)
    );
}

#[tokio::test]
async fn heartbeat_batches_active_worker_lease_extension_by_queue_like_upstream() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let first = message("task-a");
    let second = message("task-b");
    let mut third = message("task-c");
    third.queue = "default".to_owned();
    let first_lease = ProcessingLease::new(now + Duration::from_secs(5));
    let second_lease = ProcessingLease::new(now + Duration::from_secs(5));
    let third_lease = ProcessingLease::new(now + Duration::from_secs(5));
    let mut assembly = WorkerAssembly::with_parts(
        RecordingBroker::default(),
        |_task: &Task| Ok::<(), HandlerError>(()),
        DefaultRetryDelay,
        TestClock(now),
    );
    assembly.start_active_worker(&second, now, None, second_lease.clone());
    assembly.start_active_worker(&third, now, None, third_lease.clone());
    assembly.start_active_worker(&first, now, None, first_lease.clone());

    assembly.extend_active_worker_leases().await;

    assert_eq!(
        assembly.broker.lease_extension_batches,
        [
            (
                "critical".to_owned(),
                vec!["task-a".to_owned(), "task-b".to_owned()]
            ),
            ("default".to_owned(), vec!["task-c".to_owned()]),
        ]
    );
    for lease in [first_lease, second_lease, third_lease] {
        assert_eq!(
            lease.expires_at(),
            UNIX_EPOCH + Duration::from_secs(1_700_000_030)
        );
    }
}

#[tokio::test]
async fn heartbeat_skips_expired_active_worker_leases_like_upstream() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let active = message("task-id");
    let lease = ProcessingLease::new(now - Duration::from_secs(1));
    let mut assembly = WorkerAssembly::with_parts(
        RecordingBroker::default(),
        |_task: &Task| Ok::<(), HandlerError>(()),
        DefaultRetryDelay,
        TestClock(now),
    );
    assembly.start_active_worker(&active, now, None, lease.clone());

    assembly.extend_active_worker_leases().await;

    assert!(assembly.broker.lease_extensions.is_empty());
    assert_eq!(lease.expires_at(), now - Duration::from_secs(1));
}
