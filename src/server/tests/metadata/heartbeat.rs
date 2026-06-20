use super::*;

#[tokio::test]
async fn heartbeat_logs_and_ignores_server_metadata_write_errors() {
    let metadata = ServerMetadata::new(
        "host",
        123,
        "server-id",
        b"server-info".to_vec(),
        ["worker-a"],
        Duration::from_secs(30),
    )
    .unwrap();
    let runtime = MetadataWriteErrorRuntime::default();
    let write_calls = Arc::clone(&runtime.write_calls);
    let clear_calls = Arc::clone(&runtime.clear_calls);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();
    let logger = Arc::new(RecordingLogger::default());
    let server_logger: Arc<dyn Logger> = logger.clone();
    let mut server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_server_metadata(metadata)
            .with_logger(server_logger);

    let summary = test_support::run_until_stopped(&mut server, shutdown_rx)
        .await
        .unwrap();

    assert_eq!(summary.processed(), 0);
    assert_eq!(*write_calls.lock().await, 1);
    assert_eq!(*clear_calls.lock().await, 1);
    assert_eq!(
        logger.calls.lock().unwrap().as_slice(),
        [
            ("info".to_owned(), "Starting processing".to_owned()),
            (
                "error".to_owned(),
                "Failed to write server state data: redis down".to_owned()
            ),
            ("info".to_owned(), "Starting graceful shutdown".to_owned()),
            ("info".to_owned(), "Exiting".to_owned()),
        ]
    );
}

#[tokio::test]
async fn heartbeat_writes_server_runtime_metadata_snapshot() {
    let metadata = ServerMetadata::for_current_process(&["critical".to_owned()]);
    let runtime = SnapshotMetadataRuntime::default();
    let metadata_writes = Arc::clone(&runtime.metadata_writes);
    let lease_extensions = Arc::clone(&runtime.lease_extensions);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();
    let mut server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_server_metadata(metadata);

    test_support::run_until_stopped(&mut server, shutdown_rx)
        .await
        .unwrap();

    let writes = metadata_writes.lock().await;
    let server_info = pb::asynq::ServerInfo::decode(writes[0].server_info()).unwrap();
    let worker = pb::asynq::WorkerInfo::decode(writes[0].workers()[0].as_slice()).unwrap();
    assert_eq!(server_info.active_worker_count, 1);
    assert_eq!(writes[0].workers().len(), 1);
    assert_eq!(worker.task_id, "task-id");
    assert_eq!(worker.task_type, "email:welcome");
    assert_eq!(worker.queue, "critical");
    assert_eq!(*lease_extensions.lock().await, 1);
}

#[tokio::test]
async fn refreshes_server_metadata_on_heartbeat_interval() {
    let metadata = ServerMetadata::new(
        "host",
        123,
        "server-id",
        b"server-info".to_vec(),
        ["worker-a"],
        Duration::from_secs(30),
    )
    .unwrap();
    let runtime = recording_runtime();
    let metadata_writes = Arc::clone(&runtime.metadata_writes);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut server = test_support::server_with_sleeper(runtime, ["critical"], TokioSleeper)
        .unwrap()
        .with_server_metadata(metadata)
        .with_metadata_heartbeat_interval(Duration::from_millis(10))
        .with_idle_sleep(Duration::from_millis(50));

    let handle =
        tokio::spawn(
            async move { test_support::run_until_stopped(&mut server, shutdown_rx).await },
        );
    tokio::time::sleep(Duration::from_millis(35)).await;
    shutdown_tx.send(true).unwrap();
    handle.await.unwrap().unwrap();

    let writes = metadata_writes.lock().await;
    assert!(writes.len() >= 2);
    assert!(
        writes
            .iter()
            .all(|metadata| metadata.ttl() == Duration::from_millis(20))
    );
}
