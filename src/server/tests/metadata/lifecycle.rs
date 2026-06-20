use super::*;

#[derive(Debug, Clone)]
struct DrainingMetadataRuntime {
    run_started: watch::Sender<bool>,
    finish_run: watch::Receiver<bool>,
    metadata_writes: Arc<Mutex<Vec<ServerMetadata>>>,
}

#[async_trait]
impl WorkerTaskRunner for DrainingMetadataRuntime {
    async fn run_task_once(&mut self, _queues: &[String]) -> Result<WorkerRun, ProcessingError> {
        let _ = self.run_started.send(true);
        while !*self.finish_run.borrow() {
            if self.finish_run.changed().await.is_err() {
                break;
            }
        }
        Ok(WorkerRun::Completed {
            task_id: "task-id".to_owned(),
        })
    }
}

impl WorkerFactory for DrainingMetadataRuntime {
    type WorkerHandle = Self;

    fn worker(&self) -> Worker<Self::WorkerHandle> {
        Worker::new(self.clone())
    }
}

impl BorrowedWorkerFactory for DrainingMetadataRuntime {
    type WorkerHandle<'a>
        = Self
    where
        Self: 'a;

    fn borrowed_worker(&mut self) -> Worker<Self::WorkerHandle<'_>> {
        Worker::new(self.clone())
    }
}

#[async_trait]
impl ServerHeartbeatStore for DrainingMetadataRuntime {
    async fn write_server_metadata(
        &mut self,
        metadata: &ServerMetadata,
    ) -> Result<(), MetadataError> {
        self.metadata_writes.lock().await.push(metadata.clone());
        Ok(())
    }

    async fn clear_server_metadata(
        &mut self,
        _metadata: &ServerMetadata,
    ) -> Result<(), MetadataError> {
        Ok(())
    }
}

#[async_trait]
impl ServerConnection for DrainingMetadataRuntime {
    async fn ping(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), BrokerError> {
        Ok(())
    }
}

impl ServerClock for DrainingMetadataRuntime {
    fn runtime_now(&self) -> SystemTime {
        SystemTime::now()
    }
}

impl ServerRuntimeStateAttach for DrainingMetadataRuntime {
    fn attach_server_runtime(&mut self, _runtime: &ServerRuntimeState) {}
}

#[async_trait]
impl ServerShutdown for DrainingMetadataRuntime {
    async fn shutdown(&mut self) -> Result<(), ProcessingError> {
        Ok(())
    }
}

#[async_trait]
impl ServerLeaseExtender for DrainingMetadataRuntime {
    async fn extend_leases(
        &mut self,
        _queue: &str,
        _task_ids: &[String],
    ) -> Result<crate::LeaseExtension, crate::LeaseError> {
        Ok(crate::LeaseExtension::new(SystemTime::now()))
    }
}

#[async_trait]
impl ServerMaintenanceRunner for DrainingMetadataRuntime {
    async fn run_forwarder_maintenance(
        &mut self,
        _queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        Ok(ServerMaintenanceRun::default())
    }

    async fn run_recoverer_maintenance(
        &mut self,
        _queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        Ok(ServerMaintenanceRun::default())
    }

    async fn run_janitor_maintenance(
        &mut self,
        _queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        Ok(ServerMaintenanceRun::default())
    }
}

#[async_trait]
impl ServerSyncStore for DrainingMetadataRuntime {
    async fn apply_pending_sync_operation(
        &mut self,
        _operation: &PendingSyncOperation,
    ) -> Result<(), ()> {
        Ok(())
    }
}

#[tokio::test]
async fn writes_and_clears_server_metadata_around_run_loop() {
    let metadata = ServerMetadata::new(
        "host",
        123,
        "server-id",
        b"server-info".to_vec(),
        ["worker-a", "worker-b"],
        Duration::from_secs(30),
    )
    .unwrap();
    let runtime = recording_runtime();
    runtime
        .results
        .lock()
        .await
        .push(Ok(WorkerRun::NoProcessableTask));
    let metadata_writes = Arc::clone(&runtime.metadata_writes);
    let metadata_clears = Arc::clone(&runtime.metadata_clears);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_server_metadata(metadata.clone());

    let handle =
        tokio::spawn(
            async move { test_support::run_until_stopped(&mut server, shutdown_rx).await },
        );
    tokio::task::yield_now().await;
    shutdown_tx.send(true).unwrap();
    handle.await.unwrap().unwrap();

    let expected_write = metadata.with_ttl(DEFAULT_SERVER_METADATA_TTL);
    assert_eq!(metadata_writes.lock().await.as_slice(), [expected_write]);
    assert_eq!(metadata_clears.lock().await.as_slice(), [metadata]);
}

#[tokio::test]
async fn parallel_heartbeat_continues_during_worker_shutdown_like_upstream() {
    let metadata = ServerMetadata::new(
        "host",
        123,
        "server-id",
        b"server-info".to_vec(),
        ["worker-a"],
        Duration::from_secs(30),
    )
    .unwrap();
    let (run_started_tx, mut run_started_rx) = watch::channel(false);
    let (finish_run_tx, finish_run_rx) = watch::channel(false);
    let metadata_writes = Arc::new(Mutex::new(Vec::new()));
    let runtime = DrainingMetadataRuntime {
        run_started: run_started_tx,
        finish_run: finish_run_rx,
        metadata_writes: Arc::clone(&metadata_writes),
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let server = test_support::server_with_sleeper(runtime, ["critical"], TokioSleeper)
        .unwrap()
        .with_server_metadata(metadata)
        .with_metadata_heartbeat_interval(Duration::from_millis(10));

    let handle = tokio::spawn(async move {
        test_support::run_until_stopped_parallel(server, 1, shutdown_rx).await
    });
    while !*run_started_rx.borrow() {
        run_started_rx.changed().await.unwrap();
    }
    wait_until(Duration::from_secs(1), || {
        let metadata_writes = Arc::clone(&metadata_writes);
        async move { !metadata_writes.lock().await.is_empty() }
    })
    .await;

    let writes_before_shutdown = metadata_writes.lock().await.len();
    shutdown_tx.send(true).unwrap();
    wait_until(Duration::from_secs(1), || {
        let metadata_writes = Arc::clone(&metadata_writes);
        async move { metadata_writes.lock().await.len() > writes_before_shutdown }
    })
    .await;

    finish_run_tx.send(true).unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn parallel_run_manages_metadata_once_for_all_workers() {
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
    runtime
        .results
        .lock()
        .await
        .push(Ok(WorkerRun::NoProcessableTask));
    let metadata_writes = Arc::clone(&runtime.metadata_writes);
    let metadata_clears = Arc::clone(&runtime.metadata_clears);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();
    let server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_server_metadata(metadata.clone())
            .with_metadata_heartbeat_interval(Duration::from_secs(10));

    test_support::run_until_stopped_parallel(server, 3, shutdown_rx)
        .await
        .unwrap();

    let expected_write = metadata.with_ttl(Duration::from_secs(20));
    assert_eq!(metadata_writes.lock().await.as_slice(), [expected_write]);
    assert_eq!(metadata_clears.lock().await.as_slice(), [metadata]);
}

#[tokio::test]
async fn shutdown_logs_and_ignores_server_metadata_clear_errors() {
    let metadata = ServerMetadata::new(
        "host",
        123,
        "server-id",
        b"server-info".to_vec(),
        ["worker-a"],
        Duration::from_secs(30),
    )
    .unwrap();
    let runtime = MetadataClearErrorRuntime::default();
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
    assert_eq!(*clear_calls.lock().await, 1);
    assert_eq!(
        logger.calls.lock().unwrap().as_slice(),
        [
            ("info".to_owned(), "Starting processing".to_owned()),
            ("info".to_owned(), "Starting graceful shutdown".to_owned()),
            (
                "error".to_owned(),
                "Failed to clear server state: redis down".to_owned()
            ),
            ("info".to_owned(), "Exiting".to_owned()),
        ]
    );
}
