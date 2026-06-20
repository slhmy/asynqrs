use super::*;

#[tokio::test]
async fn shutdown_waits_for_in_flight_run_within_timeout() {
    let (run_started_tx, mut run_started_rx) = watch::channel(false);
    let (finish_run_tx, finish_run_rx) = watch::channel(false);
    let shutdown_calls = Arc::new(Mutex::new(0));
    let runtime = GracefulShutdownRuntime {
        run_started: run_started_tx,
        finish_run: finish_run_rx,
        shutdown_calls: Arc::clone(&shutdown_calls),
        sync_calls: Arc::new(Mutex::new(0)),
        runtime_state: None,
    };
    let mut server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle =
        tokio::spawn(
            async move { test_support::run_until_stopped(&mut server, shutdown_rx).await },
        );

    while !*run_started_rx.borrow() {
        run_started_rx.changed().await.unwrap();
    }
    shutdown_tx.send(true).unwrap();
    tokio::task::yield_now().await;
    assert!(!handle.is_finished());

    finish_run_tx.send(true).unwrap();
    let summary = tokio::time::timeout(Duration::from_millis(100), handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(summary.processed(), 1);
    assert_eq!(*shutdown_calls.lock().await, 1);
}

#[tokio::test]
async fn shutdown_times_out_in_flight_run_then_calls_runtime_shutdown() {
    let runtime = BlockingRuntime::default();
    let shutdown_calls = Arc::clone(&runtime.shutdown_calls);
    let mut server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap();
    server = server.with_shutdown_timeout(Duration::from_millis(5));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle =
        tokio::spawn(
            async move { test_support::run_until_stopped(&mut server, shutdown_rx).await },
        );

    tokio::task::yield_now().await;
    shutdown_tx.send(true).unwrap();
    let summary = tokio::time::timeout(Duration::from_millis(100), handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(summary.processed(), 0);
    assert_eq!(*shutdown_calls.lock().await, 1);
}

#[tokio::test]
async fn parallel_shutdown_requeues_timed_out_worker_from_pool_boundary() {
    let (blocked_started_tx, mut blocked_started_rx) = watch::channel(false);
    let (completed_tx, mut completed_rx) = watch::channel(false);
    let (_finish_blocked_tx, finish_blocked_rx) = watch::channel(false);
    let shutdown_calls = Arc::new(Mutex::new(0));
    let runtime = MixedShutdownRuntime {
        behaviors: Arc::new(Mutex::new(
            [
                MixedShutdownBehavior::Block,
                MixedShutdownBehavior::Complete,
            ]
            .into(),
        )),
        blocked_started: blocked_started_tx,
        completed: completed_tx,
        finish_blocked: finish_blocked_rx,
        shutdown_calls: Arc::clone(&shutdown_calls),
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let server = test_support::server_with_sleeper(runtime, ["critical"], TokioSleeper)
        .unwrap()
        .with_worker_count(2)
        .with_idle_sleep(Duration::from_millis(1))
        .with_shutdown_timeout(Duration::from_millis(5));

    let handle = tokio::spawn(async move {
        test_support::run_until_stopped_configured_parallel(server, shutdown_rx).await
    });
    while !*blocked_started_rx.borrow() {
        blocked_started_rx.changed().await.unwrap();
    }
    while !*completed_rx.borrow() {
        completed_rx.changed().await.unwrap();
    }
    shutdown_tx.send(true).unwrap();
    let summary = tokio::time::timeout(Duration::from_millis(100), handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(summary.processed(), 1);
    assert_eq!(summary.completed(), 1);
    assert_eq!(*shutdown_calls.lock().await, 2);
}

#[tokio::test]
async fn shutdown_timeout_bounds_runtime_shutdown() {
    let mut server = test_support::server_with_sleeper(
        BlockingShutdownRuntime,
        ["critical"],
        RecordingSleeper::default(),
    )
    .unwrap()
    .with_shutdown_timeout(Duration::from_millis(5));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();

    let error = test_support::run_until_stopped(&mut server, shutdown_rx)
        .await
        .unwrap_err();

    assert_eq!(error, ServerError::ShutdownTimedOut);
}

#[derive(Debug, Clone)]
struct MixedShutdownRuntime {
    behaviors: Arc<Mutex<std::collections::VecDeque<MixedShutdownBehavior>>>,
    blocked_started: watch::Sender<bool>,
    completed: watch::Sender<bool>,
    finish_blocked: watch::Receiver<bool>,
    shutdown_calls: Arc<Mutex<usize>>,
}

#[derive(Debug, Clone, Copy)]
enum MixedShutdownBehavior {
    Block,
    Complete,
}

#[async_trait]
impl WorkerTaskRunner for MixedShutdownRuntime {
    async fn run_task_once(&mut self, _queues: &[String]) -> Result<WorkerRun, ProcessingError> {
        let behavior = self.behaviors.lock().await.pop_front();
        match behavior {
            Some(MixedShutdownBehavior::Block) => {
                let _ = self.blocked_started.send(true);
                while !*self.finish_blocked.borrow() {
                    if self.finish_blocked.changed().await.is_err() {
                        break;
                    }
                }
                Ok(WorkerRun::Completed {
                    task_id: "blocked-task-id".to_owned(),
                })
            }
            Some(MixedShutdownBehavior::Complete) => {
                let _ = self.completed.send(true);
                Ok(WorkerRun::Completed {
                    task_id: "completed-task-id".to_owned(),
                })
            }
            None => Ok(WorkerRun::NoProcessableTask),
        }
    }
}

impl WorkerFactory for MixedShutdownRuntime {
    type WorkerHandle = Self;

    fn worker(&self) -> Worker<Self::WorkerHandle> {
        Worker::new(self.clone())
    }
}

impl BorrowedWorkerFactory for MixedShutdownRuntime {
    type WorkerHandle<'a>
        = Self
    where
        Self: 'a;

    fn borrowed_worker(&mut self) -> Worker<Self::WorkerHandle<'_>> {
        Worker::new(self.clone())
    }
}

#[async_trait]
impl ServerShutdown for MixedShutdownRuntime {
    async fn shutdown(&mut self) -> Result<(), ProcessingError> {
        *self.shutdown_calls.lock().await += 1;
        Ok(())
    }
}

#[async_trait]
impl ServerConnection for MixedShutdownRuntime {
    async fn ping(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), BrokerError> {
        Ok(())
    }
}

impl ServerClock for MixedShutdownRuntime {
    fn runtime_now(&self) -> SystemTime {
        SystemTime::now()
    }
}

impl ServerRuntimeStateAttach for MixedShutdownRuntime {
    fn attach_server_runtime(&mut self, _runtime: &ServerRuntimeState) {}
}

#[async_trait]
impl ServerHeartbeatStore for MixedShutdownRuntime {
    async fn write_server_metadata(
        &mut self,
        _metadata: &ServerMetadata,
    ) -> Result<(), MetadataError> {
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
impl ServerLeaseExtender for MixedShutdownRuntime {
    async fn extend_leases(
        &mut self,
        _queue: &str,
        _task_ids: &[String],
    ) -> Result<crate::LeaseExtension, crate::LeaseError> {
        Ok(crate::LeaseExtension::new(SystemTime::now()))
    }
}

#[async_trait]
impl ServerMaintenanceRunner for MixedShutdownRuntime {
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
impl ServerSyncStore for MixedShutdownRuntime {
    async fn apply_pending_sync_operation(
        &mut self,
        _operation: &PendingSyncOperation,
    ) -> Result<(), ()> {
        Ok(())
    }
}
