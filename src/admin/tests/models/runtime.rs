use super::*;

#[test]
fn runtime_info_models_expose_asynq_field_accessors() {
    let started = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let deadline = UNIX_EPOCH + Duration::from_secs(1_700_000_600);
    let next_started = started + Duration::from_secs(30);
    let next_deadline = deadline + Duration::from_secs(30);
    let mut worker = WorkerInfo::new(
        "task-id".to_owned(),
        "email:welcome".to_owned(),
        b"payload".to_vec(),
        "critical".to_owned(),
        started,
        deadline,
    );

    worker.task_id_mut().push_str("-a");
    worker.task_id_mut().push_str("-b");
    worker.task_type_mut().push_str(":v1");
    worker.task_type_mut().push_str(":v2");
    worker.task_type_mut().push_str(":v3");
    worker.task_payload_mut().extend_from_slice(b"-a");
    worker.task_payload_mut().extend_from_slice(b"-b");
    worker.task_payload_mut().extend_from_slice(b"-c");
    worker.queue_mut().push_str("-tenant");
    worker.queue_mut().push_str("-a");
    *worker.started_mut() = next_started;
    *worker.started_mut() = started;
    *worker.deadline_mut() = next_deadline;
    *worker.deadline_mut() = deadline;

    let mut server = ServerInfo::new(ServerInfoParts {
        id: "server-id".to_owned(),
        host: "host.local".to_owned(),
        pid: 1234,
        concurrency: 8,
        queues: HashMap::from([("critical".to_owned(), 5), ("default".to_owned(), 1)]),
        strict_priority: true,
        started,
        status: "active".to_owned(),
        active_workers: vec![worker],
    });

    server.id_mut().push_str("-a");
    server.id_mut().push_str("-b");
    server.host_mut().push_str(".cluster");
    server.host_mut().push_str(".local");
    *server.pid_mut() += 1;
    *server.pid_mut() += 2;
    *server.concurrency_mut() += 3;
    *server.concurrency_mut() += 4;
    server.queues_mut().insert("bulk".to_owned(), 2);
    server.queues_mut().insert("critical".to_owned(), 7);
    *server.strict_priority_mut() = false;
    *server.strict_priority_mut() = true;
    *server.started_mut() = next_started;
    *server.started_mut() = started;
    server.status_mut().push_str("-draining");
    server.status_mut().push_str("-ready");
    server.active_workers_mut()[0]
        .task_id_mut()
        .push_str("-server");
    server.active_workers_mut()[0]
        .queue_mut()
        .push_str("-server");

    assert_eq!(server.id(), "server-id-a-b");
    assert_eq!(server.id(), "server-id-a-b");
    assert_eq!(server.host(), "host.local.cluster.local");
    assert_eq!(server.host(), "host.local.cluster.local");
    assert_eq!(server.pid(), 1237);
    assert_eq!(server.pid(), 1237);
    assert_eq!(server.concurrency(), 15);
    assert_eq!(server.concurrency(), 15);
    assert_eq!(server.queues().get("critical"), Some(&7));
    assert_eq!(server.queues().get("bulk"), Some(&2));
    assert!(server.strict_priority());
    assert!(server.strict_priority());
    assert_eq!(server.started(), started);
    assert_eq!(server.started(), started);
    assert_eq!(server.status(), "active-draining-ready");
    assert_eq!(server.status(), "active-draining-ready");
    assert_eq!(server.active_workers().len(), 1);
    assert_eq!(server.active_workers().len(), 1);

    let worker = &server.active_workers()[0];
    assert_eq!(worker.task_id(), "task-id-a-b-server");
    assert_eq!(worker.task_id(), "task-id-a-b-server");
    assert_eq!(worker.task_type(), "email:welcome:v1:v2:v3");
    assert_eq!(worker.task_type(), "email:welcome:v1:v2:v3");
    assert_eq!(worker.task_type(), "email:welcome:v1:v2:v3");
    assert_eq!(worker.task_payload(), b"payload-a-b-c");
    assert_eq!(worker.task_payload(), b"payload-a-b-c");
    assert_eq!(worker.task_payload(), b"payload-a-b-c");
    assert_eq!(worker.queue(), "critical-tenant-a-server");
    assert_eq!(worker.queue(), "critical-tenant-a-server");
    assert_eq!(worker.started(), started);
    assert_eq!(worker.started(), started);
    assert_eq!(worker.deadline(), deadline);
    assert_eq!(worker.deadline(), deadline);
}
