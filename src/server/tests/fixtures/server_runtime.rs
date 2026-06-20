use super::*;
use crate::server::{PendingSyncOperation, ProcessingLease, ServerRuntimeState, test_support};
use crate::{LeaseError, LeaseExtension};

pub(in crate::server::tests) fn recording_runtime() -> RecordingRuntime {
    RecordingRuntime {
        results: Arc::new(Mutex::new(Vec::new())),
        queue_calls: Arc::new(Mutex::new(Vec::new())),
        maintenance_calls: Arc::new(Mutex::new(Vec::new())),
        shutdown_calls: Arc::new(Mutex::new(0)),
        sync_calls: Arc::new(Mutex::new(0)),
        stop_after_run_once: None,
        metadata_writes: Arc::new(Mutex::new(Vec::new())),
        metadata_clears: Arc::new(Mutex::new(Vec::new())),
        runtime_state: None,
    }
}

pub(in crate::server::tests) fn pending_sync_runtime_state() -> ServerRuntimeState {
    let mut message =
        pb::asynq::TaskMessage::from_task(&crate::Task::new("email:welcome", b"payload".to_vec()));
    message.id = "sync-task-id".to_owned();
    message.queue = "critical".to_owned();
    test_support::runtime_state_with_pending_complete(
        message,
        SystemTime::now() + Duration::from_secs(30),
        "Could not sync task; Will retry syncing".to_owned(),
    )
}

pub(in crate::server::tests) async fn shutdown_noop() -> Result<(), ProcessingError> {
    Ok(())
}

async fn connection_ping_noop() -> Result<(), String> {
    Ok(())
}

fn connection_close_noop() -> Result<(), BrokerError> {
    Ok(())
}

async fn sync_noop(_operation: &PendingSyncOperation) -> Result<(), ()> {
    Ok(())
}

async fn metadata_noop(_metadata: &ServerMetadata) -> Result<(), MetadataError> {
    Ok(())
}

async fn lease_noop(_queue: &str, _task_ids: &[String]) -> Result<LeaseExtension, LeaseError> {
    Ok(LeaseExtension::new(SystemTime::now()))
}

async fn maintenance_noop(_queues: &[String]) -> Result<ServerMaintenanceRun, ProcessingError> {
    Ok(ServerMaintenanceRun::default())
}

fn runtime_now_wall_clock() -> SystemTime {
    SystemTime::now()
}

macro_rules! impl_worker_factory_for_fixture {
    ($($runtime:ty),+ $(,)?) => {
        $(
            impl WorkerFactory for $runtime {
                type WorkerHandle = Self;

                fn worker(&self) -> Worker<Self::WorkerHandle> {
                    Worker::new(self.clone())
                }
            }
        )+
    };
}

macro_rules! impl_borrowed_worker_factory_for_fixture {
    ($($runtime:ty),+ $(,)?) => {
        $(
            impl BorrowedWorkerFactory for $runtime {
                type WorkerHandle<'a> = Self where Self: 'a;

                fn borrowed_worker(&mut self) -> Worker<Self::WorkerHandle<'_>> {
                    Worker::new(self.clone())
                }
            }
        )+
    };
}

#[derive(Debug, Clone)]
pub(in crate::server::tests) struct RecordingRuntime {
    pub(in crate::server::tests) results: Arc<Mutex<Vec<Result<WorkerRun, ProcessingError>>>>,
    pub(in crate::server::tests) queue_calls: Arc<Mutex<Vec<Vec<String>>>>,
    pub(in crate::server::tests) maintenance_calls: Arc<Mutex<Vec<Vec<String>>>>,
    pub(in crate::server::tests) shutdown_calls: Arc<Mutex<usize>>,
    pub(in crate::server::tests) sync_calls: Arc<Mutex<usize>>,
    pub(in crate::server::tests) stop_after_run_once: Option<watch::Sender<bool>>,
    pub(in crate::server::tests) metadata_writes: Arc<Mutex<Vec<ServerMetadata>>>,
    pub(in crate::server::tests) metadata_clears: Arc<Mutex<Vec<ServerMetadata>>>,
    pub(in crate::server::tests) runtime_state: Option<ServerRuntimeState>,
}

#[async_trait]
impl WorkerTaskRunner for RecordingRuntime {
    async fn run_task_once(&mut self, queues: &[String]) -> Result<WorkerRun, ProcessingError> {
        self.queue_calls.lock().await.push(queues.to_vec());
        let mut results = self.results.lock().await;
        let result = if results.is_empty() {
            Ok(WorkerRun::NoProcessableTask)
        } else {
            results.remove(0)
        };
        if let Some(stop_after_run_once) = &self.stop_after_run_once {
            let _ = stop_after_run_once.send(true);
        }
        result
    }
}

impl_worker_factory_for_fixture!(
    RecordingRuntime,
    CloseTrackingRuntime,
    SnapshotMetadataRuntime,
    RecordingPingRuntime,
    FlakyMaintenanceRuntime,
    MetadataWriteErrorRuntime,
    MetadataClearErrorRuntime,
    BlockingRuntime,
    GracefulShutdownRuntime,
    BlockingShutdownRuntime,
);

impl_borrowed_worker_factory_for_fixture!(
    RecordingRuntime,
    CloseTrackingRuntime,
    SnapshotMetadataRuntime,
    RecordingPingRuntime,
    FlakyMaintenanceRuntime,
    MetadataWriteErrorRuntime,
    MetadataClearErrorRuntime,
    BlockingRuntime,
    GracefulShutdownRuntime,
    BlockingShutdownRuntime,
);

#[async_trait]
impl ServerShutdown for RecordingRuntime {
    async fn shutdown(&mut self) -> Result<(), ProcessingError> {
        *self.shutdown_calls.lock().await += 1;
        Ok(())
    }
}

#[async_trait]
impl ServerMaintenanceRunner for RecordingRuntime {
    async fn run_forwarder_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        self.maintenance_calls.lock().await.push(queues.to_vec());
        Ok(ServerMaintenanceRun::new(1, 2, 0, 0, 0))
    }

    async fn run_recoverer_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        self.maintenance_calls.lock().await.push(queues.to_vec());
        Ok(ServerMaintenanceRun::new(0, 0, 3, 4, 0))
    }

    async fn run_janitor_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        self.maintenance_calls.lock().await.push(queues.to_vec());
        Ok(ServerMaintenanceRun::new(0, 0, 0, 0, 5))
    }
}

#[async_trait]
impl ServerHeartbeatStore for RecordingRuntime {
    async fn write_server_metadata(
        &mut self,
        metadata: &ServerMetadata,
    ) -> Result<(), MetadataError> {
        self.metadata_writes.lock().await.push(metadata.clone());
        Ok(())
    }

    async fn clear_server_metadata(
        &mut self,
        metadata: &ServerMetadata,
    ) -> Result<(), MetadataError> {
        self.metadata_clears.lock().await.push(metadata.clone());
        Ok(())
    }
}

#[async_trait]
impl ServerConnection for RecordingRuntime {
    async fn ping(&mut self) -> Result<(), String> {
        connection_ping_noop().await
    }

    fn close(&mut self) -> Result<(), BrokerError> {
        connection_close_noop()
    }
}

#[async_trait]
impl ServerSyncStore for RecordingRuntime {
    async fn apply_pending_sync_operation(
        &mut self,
        _operation: &PendingSyncOperation,
    ) -> Result<(), ()> {
        *self.sync_calls.lock().await += 1;
        Ok(())
    }
}

impl ServerClock for RecordingRuntime {
    fn runtime_now(&self) -> SystemTime {
        runtime_now_wall_clock()
    }
}

impl ServerRuntimeStateAttach for RecordingRuntime {
    fn attach_server_runtime(&mut self, runtime: &ServerRuntimeState) {
        if let Some(existing) = self.runtime_state.take() {
            let pending_sync = existing.pending_sync();
            while let Some(operation) = pending_sync.pop() {
                runtime.pending_sync().push(operation);
            }
        }
        self.runtime_state = Some(runtime.clone());
    }
}

#[async_trait]
impl ServerLeaseExtender for RecordingRuntime {
    async fn extend_leases(
        &mut self,
        queue: &str,
        task_ids: &[String],
    ) -> Result<LeaseExtension, LeaseError> {
        lease_noop(queue, task_ids).await
    }
}

#[derive(Debug, Clone, Default)]
pub(in crate::server::tests) struct CloseTrackingRuntime {
    pub(in crate::server::tests) close_calls: Arc<Mutex<usize>>,
}

#[async_trait]
impl WorkerTaskRunner for CloseTrackingRuntime {
    async fn run_task_once(&mut self, _queues: &[String]) -> Result<WorkerRun, ProcessingError> {
        Ok(WorkerRun::NoProcessableTask)
    }
}

#[async_trait]
impl ServerConnection for CloseTrackingRuntime {
    async fn ping(&mut self) -> Result<(), String> {
        connection_ping_noop().await
    }

    fn close(&mut self) -> Result<(), BrokerError> {
        *self.close_calls.try_lock().expect("close calls lock held") += 1;
        Ok(())
    }
}

impl ServerClock for CloseTrackingRuntime {
    fn runtime_now(&self) -> SystemTime {
        runtime_now_wall_clock()
    }
}

impl ServerRuntimeStateAttach for CloseTrackingRuntime {
    fn attach_server_runtime(&mut self, _runtime: &ServerRuntimeState) {}
}

#[async_trait]
impl ServerShutdown for CloseTrackingRuntime {
    async fn shutdown(&mut self) -> Result<(), ProcessingError> {
        shutdown_noop().await
    }
}

#[async_trait]
impl ServerHeartbeatStore for CloseTrackingRuntime {
    async fn write_server_metadata(
        &mut self,
        metadata: &ServerMetadata,
    ) -> Result<(), MetadataError> {
        metadata_noop(metadata).await
    }

    async fn clear_server_metadata(
        &mut self,
        metadata: &ServerMetadata,
    ) -> Result<(), MetadataError> {
        metadata_noop(metadata).await
    }
}

#[async_trait]
impl ServerLeaseExtender for CloseTrackingRuntime {
    async fn extend_leases(
        &mut self,
        queue: &str,
        task_ids: &[String],
    ) -> Result<LeaseExtension, LeaseError> {
        lease_noop(queue, task_ids).await
    }
}

#[async_trait]
impl ServerMaintenanceRunner for CloseTrackingRuntime {
    async fn run_forwarder_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        maintenance_noop(queues).await
    }

    async fn run_recoverer_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        maintenance_noop(queues).await
    }

    async fn run_janitor_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        maintenance_noop(queues).await
    }
}

#[async_trait]
impl ServerSyncStore for CloseTrackingRuntime {
    async fn apply_pending_sync_operation(
        &mut self,
        operation: &PendingSyncOperation,
    ) -> Result<(), ()> {
        sync_noop(operation).await
    }
}

#[derive(Debug, Clone)]
pub(in crate::server::tests) struct SnapshotMetadataRuntime {
    pub(in crate::server::tests) metadata_writes: Arc<Mutex<Vec<ServerMetadata>>>,
    pub(in crate::server::tests) lease_extensions: Arc<Mutex<usize>>,
    runtime_state: ServerRuntimeState,
    now: SystemTime,
}

impl Default for SnapshotMetadataRuntime {
    fn default() -> Self {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let mut message = pb::asynq::TaskMessage::from_task(&crate::Task::new(
            "email:welcome",
            b"payload".to_vec(),
        ));
        message.id = "task-id".to_owned();
        message.queue = "critical".to_owned();
        Self {
            metadata_writes: Arc::new(Mutex::new(Vec::new())),
            lease_extensions: Arc::new(Mutex::new(0)),
            runtime_state: test_support::runtime_state_with_active_worker(
                message,
                now,
                Some(now + Duration::from_secs(30)),
                now + Duration::from_secs(30),
            ),
            now,
        }
    }
}

#[async_trait]
impl WorkerTaskRunner for SnapshotMetadataRuntime {
    async fn run_task_once(&mut self, _queues: &[String]) -> Result<WorkerRun, ProcessingError> {
        Ok(WorkerRun::NoProcessableTask)
    }
}

#[async_trait]
impl ServerLeaseExtender for SnapshotMetadataRuntime {
    async fn extend_leases(
        &mut self,
        _queue: &str,
        _task_ids: &[String],
    ) -> Result<LeaseExtension, LeaseError> {
        *self.lease_extensions.lock().await += 1;
        Ok(LeaseExtension::new(self.now + Duration::from_secs(30)))
    }
}

#[async_trait]
impl ServerHeartbeatStore for SnapshotMetadataRuntime {
    async fn write_server_metadata(
        &mut self,
        metadata: &ServerMetadata,
    ) -> Result<(), MetadataError> {
        self.metadata_writes.lock().await.push(metadata.clone());
        Ok(())
    }

    async fn clear_server_metadata(
        &mut self,
        metadata: &ServerMetadata,
    ) -> Result<(), MetadataError> {
        metadata_noop(metadata).await
    }
}

impl ServerClock for SnapshotMetadataRuntime {
    fn runtime_now(&self) -> SystemTime {
        self.now
    }
}

impl ServerRuntimeStateAttach for SnapshotMetadataRuntime {
    fn attach_server_runtime(&mut self, runtime: &ServerRuntimeState) {
        let mut message = pb::asynq::TaskMessage::from_task(&crate::Task::new(
            "email:welcome",
            b"payload".to_vec(),
        ));
        message.id = "task-id".to_owned();
        message.queue = "critical".to_owned();
        runtime.insert_active_worker(
            message,
            self.now,
            Some(self.now + Duration::from_secs(30)),
            ProcessingLease::new(self.now + Duration::from_secs(30)),
        );
        self.runtime_state = runtime.clone();
    }
}

#[async_trait]
impl ServerConnection for SnapshotMetadataRuntime {
    async fn ping(&mut self) -> Result<(), String> {
        connection_ping_noop().await
    }

    fn close(&mut self) -> Result<(), BrokerError> {
        connection_close_noop()
    }
}

#[async_trait]
impl ServerShutdown for SnapshotMetadataRuntime {
    async fn shutdown(&mut self) -> Result<(), ProcessingError> {
        shutdown_noop().await
    }
}

#[async_trait]
impl ServerMaintenanceRunner for SnapshotMetadataRuntime {
    async fn run_forwarder_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        maintenance_noop(queues).await
    }

    async fn run_recoverer_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        maintenance_noop(queues).await
    }

    async fn run_janitor_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        maintenance_noop(queues).await
    }
}

#[async_trait]
impl ServerSyncStore for SnapshotMetadataRuntime {
    async fn apply_pending_sync_operation(
        &mut self,
        operation: &PendingSyncOperation,
    ) -> Result<(), ()> {
        sync_noop(operation).await
    }
}

#[derive(Debug, Clone, Default)]
pub(in crate::server::tests) struct RecordingPingRuntime {
    pub(in crate::server::tests) ping_error: Option<String>,
    pub(in crate::server::tests) ping_calls: Arc<Mutex<usize>>,
}

#[async_trait]
impl WorkerTaskRunner for RecordingPingRuntime {
    async fn run_task_once(&mut self, _queues: &[String]) -> Result<WorkerRun, ProcessingError> {
        Ok(WorkerRun::NoProcessableTask)
    }
}

#[async_trait]
impl ServerConnection for RecordingPingRuntime {
    async fn ping(&mut self) -> Result<(), String> {
        *self.ping_calls.lock().await += 1;
        if let Some(error) = self.ping_error.clone() {
            Err(error)
        } else {
            Ok(())
        }
    }

    fn close(&mut self) -> Result<(), BrokerError> {
        connection_close_noop()
    }
}

impl ServerClock for RecordingPingRuntime {
    fn runtime_now(&self) -> SystemTime {
        runtime_now_wall_clock()
    }
}

impl ServerRuntimeStateAttach for RecordingPingRuntime {
    fn attach_server_runtime(&mut self, _runtime: &ServerRuntimeState) {}
}

#[async_trait]
impl ServerShutdown for RecordingPingRuntime {
    async fn shutdown(&mut self) -> Result<(), ProcessingError> {
        shutdown_noop().await
    }
}

#[async_trait]
impl ServerHeartbeatStore for RecordingPingRuntime {
    async fn write_server_metadata(
        &mut self,
        metadata: &ServerMetadata,
    ) -> Result<(), MetadataError> {
        metadata_noop(metadata).await
    }

    async fn clear_server_metadata(
        &mut self,
        metadata: &ServerMetadata,
    ) -> Result<(), MetadataError> {
        metadata_noop(metadata).await
    }
}

#[async_trait]
impl ServerLeaseExtender for RecordingPingRuntime {
    async fn extend_leases(
        &mut self,
        queue: &str,
        task_ids: &[String],
    ) -> Result<LeaseExtension, LeaseError> {
        lease_noop(queue, task_ids).await
    }
}

#[async_trait]
impl ServerMaintenanceRunner for RecordingPingRuntime {
    async fn run_forwarder_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        maintenance_noop(queues).await
    }

    async fn run_recoverer_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        maintenance_noop(queues).await
    }

    async fn run_janitor_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        maintenance_noop(queues).await
    }
}

#[async_trait]
impl ServerSyncStore for RecordingPingRuntime {
    async fn apply_pending_sync_operation(
        &mut self,
        operation: &PendingSyncOperation,
    ) -> Result<(), ()> {
        sync_noop(operation).await
    }
}

#[derive(Debug, Clone)]
pub(in crate::server::tests) struct FlakyMaintenanceRuntime {
    pub(in crate::server::tests) results: Arc<Mutex<Vec<Result<WorkerRun, ProcessingError>>>>,
    pub(in crate::server::tests) recoverer_results:
        Arc<Mutex<Vec<Result<ServerMaintenanceRun, ProcessingError>>>>,
    pub(in crate::server::tests) recoverer_calls: Arc<Mutex<usize>>,
    pub(in crate::server::tests) shutdown_calls: Arc<Mutex<usize>>,
    pub(in crate::server::tests) stop_after_run_once: Option<watch::Sender<bool>>,
}

#[async_trait]
impl WorkerTaskRunner for FlakyMaintenanceRuntime {
    async fn run_task_once(&mut self, _queues: &[String]) -> Result<WorkerRun, ProcessingError> {
        let mut results = self.results.lock().await;
        let result = if results.is_empty() {
            Ok(WorkerRun::NoProcessableTask)
        } else {
            results.remove(0)
        };
        if let Some(stop_after_run_once) = &self.stop_after_run_once {
            let _ = stop_after_run_once.send(true);
        }
        result
    }
}

#[async_trait]
impl ServerShutdown for FlakyMaintenanceRuntime {
    async fn shutdown(&mut self) -> Result<(), ProcessingError> {
        *self.shutdown_calls.lock().await += 1;
        Ok(())
    }
}

#[async_trait]
impl ServerMaintenanceRunner for FlakyMaintenanceRuntime {
    async fn run_forwarder_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        maintenance_noop(queues).await
    }

    async fn run_recoverer_maintenance(
        &mut self,
        _queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        *self.recoverer_calls.lock().await += 1;
        let mut results = self.recoverer_results.lock().await;
        if results.is_empty() {
            Ok(ServerMaintenanceRun::default())
        } else {
            results.remove(0)
        }
    }

    async fn run_janitor_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        maintenance_noop(queues).await
    }
}

#[async_trait]
impl ServerConnection for FlakyMaintenanceRuntime {
    async fn ping(&mut self) -> Result<(), String> {
        connection_ping_noop().await
    }

    fn close(&mut self) -> Result<(), BrokerError> {
        connection_close_noop()
    }
}

impl ServerClock for FlakyMaintenanceRuntime {
    fn runtime_now(&self) -> SystemTime {
        runtime_now_wall_clock()
    }
}

impl ServerRuntimeStateAttach for FlakyMaintenanceRuntime {
    fn attach_server_runtime(&mut self, _runtime: &ServerRuntimeState) {}
}

#[async_trait]
impl ServerHeartbeatStore for FlakyMaintenanceRuntime {
    async fn write_server_metadata(
        &mut self,
        metadata: &ServerMetadata,
    ) -> Result<(), MetadataError> {
        metadata_noop(metadata).await
    }

    async fn clear_server_metadata(
        &mut self,
        metadata: &ServerMetadata,
    ) -> Result<(), MetadataError> {
        metadata_noop(metadata).await
    }
}

#[async_trait]
impl ServerLeaseExtender for FlakyMaintenanceRuntime {
    async fn extend_leases(
        &mut self,
        queue: &str,
        task_ids: &[String],
    ) -> Result<LeaseExtension, LeaseError> {
        lease_noop(queue, task_ids).await
    }
}

#[async_trait]
impl ServerSyncStore for FlakyMaintenanceRuntime {
    async fn apply_pending_sync_operation(
        &mut self,
        operation: &PendingSyncOperation,
    ) -> Result<(), ()> {
        sync_noop(operation).await
    }
}

#[derive(Debug, Clone, Default)]
pub(in crate::server::tests) struct MetadataWriteErrorRuntime {
    pub(in crate::server::tests) write_calls: Arc<Mutex<usize>>,
    pub(in crate::server::tests) clear_calls: Arc<Mutex<usize>>,
}

#[async_trait]
impl WorkerTaskRunner for MetadataWriteErrorRuntime {
    async fn run_task_once(&mut self, _queues: &[String]) -> Result<WorkerRun, ProcessingError> {
        Ok(WorkerRun::NoProcessableTask)
    }
}

#[async_trait]
impl ServerHeartbeatStore for MetadataWriteErrorRuntime {
    async fn write_server_metadata(
        &mut self,
        _metadata: &ServerMetadata,
    ) -> Result<(), MetadataError> {
        *self.write_calls.lock().await += 1;
        Err(MetadataError::Other("redis down".to_owned()))
    }

    async fn clear_server_metadata(
        &mut self,
        _metadata: &ServerMetadata,
    ) -> Result<(), MetadataError> {
        *self.clear_calls.lock().await += 1;
        Ok(())
    }
}

#[async_trait]
impl ServerConnection for MetadataWriteErrorRuntime {
    async fn ping(&mut self) -> Result<(), String> {
        connection_ping_noop().await
    }

    fn close(&mut self) -> Result<(), BrokerError> {
        connection_close_noop()
    }
}

impl ServerClock for MetadataWriteErrorRuntime {
    fn runtime_now(&self) -> SystemTime {
        runtime_now_wall_clock()
    }
}

impl ServerRuntimeStateAttach for MetadataWriteErrorRuntime {
    fn attach_server_runtime(&mut self, _runtime: &ServerRuntimeState) {}
}

#[async_trait]
impl ServerShutdown for MetadataWriteErrorRuntime {
    async fn shutdown(&mut self) -> Result<(), ProcessingError> {
        shutdown_noop().await
    }
}

#[async_trait]
impl ServerLeaseExtender for MetadataWriteErrorRuntime {
    async fn extend_leases(
        &mut self,
        queue: &str,
        task_ids: &[String],
    ) -> Result<LeaseExtension, LeaseError> {
        lease_noop(queue, task_ids).await
    }
}

#[async_trait]
impl ServerMaintenanceRunner for MetadataWriteErrorRuntime {
    async fn run_forwarder_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        maintenance_noop(queues).await
    }

    async fn run_recoverer_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        maintenance_noop(queues).await
    }

    async fn run_janitor_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        maintenance_noop(queues).await
    }
}

#[async_trait]
impl ServerSyncStore for MetadataWriteErrorRuntime {
    async fn apply_pending_sync_operation(
        &mut self,
        operation: &PendingSyncOperation,
    ) -> Result<(), ()> {
        sync_noop(operation).await
    }
}

#[derive(Debug, Clone, Default)]
pub(in crate::server::tests) struct MetadataClearErrorRuntime {
    pub(in crate::server::tests) clear_calls: Arc<Mutex<usize>>,
}

#[async_trait]
impl WorkerTaskRunner for MetadataClearErrorRuntime {
    async fn run_task_once(&mut self, _queues: &[String]) -> Result<WorkerRun, ProcessingError> {
        Ok(WorkerRun::NoProcessableTask)
    }
}

#[async_trait]
impl ServerHeartbeatStore for MetadataClearErrorRuntime {
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
        *self.clear_calls.lock().await += 1;
        Err(MetadataError::Other("redis down".to_owned()))
    }
}

#[async_trait]
impl ServerConnection for MetadataClearErrorRuntime {
    async fn ping(&mut self) -> Result<(), String> {
        connection_ping_noop().await
    }

    fn close(&mut self) -> Result<(), BrokerError> {
        connection_close_noop()
    }
}

impl ServerClock for MetadataClearErrorRuntime {
    fn runtime_now(&self) -> SystemTime {
        runtime_now_wall_clock()
    }
}

impl ServerRuntimeStateAttach for MetadataClearErrorRuntime {
    fn attach_server_runtime(&mut self, _runtime: &ServerRuntimeState) {}
}

#[async_trait]
impl ServerShutdown for MetadataClearErrorRuntime {
    async fn shutdown(&mut self) -> Result<(), ProcessingError> {
        shutdown_noop().await
    }
}

#[async_trait]
impl ServerLeaseExtender for MetadataClearErrorRuntime {
    async fn extend_leases(
        &mut self,
        queue: &str,
        task_ids: &[String],
    ) -> Result<LeaseExtension, LeaseError> {
        lease_noop(queue, task_ids).await
    }
}

#[async_trait]
impl ServerMaintenanceRunner for MetadataClearErrorRuntime {
    async fn run_forwarder_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        maintenance_noop(queues).await
    }

    async fn run_recoverer_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        maintenance_noop(queues).await
    }

    async fn run_janitor_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        maintenance_noop(queues).await
    }
}

#[async_trait]
impl ServerSyncStore for MetadataClearErrorRuntime {
    async fn apply_pending_sync_operation(
        &mut self,
        operation: &PendingSyncOperation,
    ) -> Result<(), ()> {
        sync_noop(operation).await
    }
}

#[derive(Debug, Clone, Default)]
pub(in crate::server::tests) struct BlockingRuntime {
    pub(in crate::server::tests) shutdown_calls: Arc<Mutex<usize>>,
}

#[async_trait]
impl WorkerTaskRunner for BlockingRuntime {
    async fn run_task_once(&mut self, _queues: &[String]) -> Result<WorkerRun, ProcessingError> {
        std::future::pending().await
    }
}

#[async_trait]
impl ServerShutdown for BlockingRuntime {
    async fn shutdown(&mut self) -> Result<(), ProcessingError> {
        *self.shutdown_calls.lock().await += 1;
        Ok(())
    }
}

#[async_trait]
impl ServerConnection for BlockingRuntime {
    async fn ping(&mut self) -> Result<(), String> {
        connection_ping_noop().await
    }

    fn close(&mut self) -> Result<(), BrokerError> {
        connection_close_noop()
    }
}

impl ServerClock for BlockingRuntime {
    fn runtime_now(&self) -> SystemTime {
        runtime_now_wall_clock()
    }
}

impl ServerRuntimeStateAttach for BlockingRuntime {
    fn attach_server_runtime(&mut self, _runtime: &ServerRuntimeState) {}
}

#[async_trait]
impl ServerHeartbeatStore for BlockingRuntime {
    async fn write_server_metadata(
        &mut self,
        metadata: &ServerMetadata,
    ) -> Result<(), MetadataError> {
        metadata_noop(metadata).await
    }

    async fn clear_server_metadata(
        &mut self,
        metadata: &ServerMetadata,
    ) -> Result<(), MetadataError> {
        metadata_noop(metadata).await
    }
}

#[async_trait]
impl ServerLeaseExtender for BlockingRuntime {
    async fn extend_leases(
        &mut self,
        queue: &str,
        task_ids: &[String],
    ) -> Result<LeaseExtension, LeaseError> {
        lease_noop(queue, task_ids).await
    }
}

#[async_trait]
impl ServerMaintenanceRunner for BlockingRuntime {
    async fn run_forwarder_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        maintenance_noop(queues).await
    }

    async fn run_recoverer_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        maintenance_noop(queues).await
    }

    async fn run_janitor_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        maintenance_noop(queues).await
    }
}

#[async_trait]
impl ServerSyncStore for BlockingRuntime {
    async fn apply_pending_sync_operation(
        &mut self,
        operation: &PendingSyncOperation,
    ) -> Result<(), ()> {
        sync_noop(operation).await
    }
}

#[derive(Debug, Clone)]
pub(in crate::server::tests) struct GracefulShutdownRuntime {
    pub(in crate::server::tests) run_started: watch::Sender<bool>,
    pub(in crate::server::tests) finish_run: watch::Receiver<bool>,
    pub(in crate::server::tests) shutdown_calls: Arc<Mutex<usize>>,
    pub(in crate::server::tests) sync_calls: Arc<Mutex<usize>>,
    pub(in crate::server::tests) runtime_state: Option<ServerRuntimeState>,
}

#[async_trait]
impl WorkerTaskRunner for GracefulShutdownRuntime {
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

#[async_trait]
impl ServerShutdown for GracefulShutdownRuntime {
    async fn shutdown(&mut self) -> Result<(), ProcessingError> {
        *self.shutdown_calls.lock().await += 1;
        Ok(())
    }
}

#[async_trait]
impl ServerConnection for GracefulShutdownRuntime {
    async fn ping(&mut self) -> Result<(), String> {
        connection_ping_noop().await
    }

    fn close(&mut self) -> Result<(), BrokerError> {
        connection_close_noop()
    }
}

impl ServerRuntimeStateAttach for GracefulShutdownRuntime {
    fn attach_server_runtime(&mut self, runtime: &ServerRuntimeState) {
        if let Some(existing) = self.runtime_state.take() {
            let pending_sync = existing.pending_sync();
            while let Some(operation) = pending_sync.pop() {
                runtime.pending_sync().push(operation);
            }
        }
        self.runtime_state = Some(runtime.clone());
    }
}

#[async_trait]
impl ServerHeartbeatStore for GracefulShutdownRuntime {
    async fn write_server_metadata(
        &mut self,
        metadata: &ServerMetadata,
    ) -> Result<(), MetadataError> {
        metadata_noop(metadata).await
    }

    async fn clear_server_metadata(
        &mut self,
        metadata: &ServerMetadata,
    ) -> Result<(), MetadataError> {
        metadata_noop(metadata).await
    }
}

#[async_trait]
impl ServerLeaseExtender for GracefulShutdownRuntime {
    async fn extend_leases(
        &mut self,
        queue: &str,
        task_ids: &[String],
    ) -> Result<LeaseExtension, LeaseError> {
        lease_noop(queue, task_ids).await
    }
}

#[async_trait]
impl ServerMaintenanceRunner for GracefulShutdownRuntime {
    async fn run_forwarder_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        maintenance_noop(queues).await
    }

    async fn run_recoverer_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        maintenance_noop(queues).await
    }

    async fn run_janitor_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        maintenance_noop(queues).await
    }
}

#[async_trait]
impl ServerSyncStore for GracefulShutdownRuntime {
    async fn apply_pending_sync_operation(
        &mut self,
        _operation: &PendingSyncOperation,
    ) -> Result<(), ()> {
        *self.sync_calls.lock().await += 1;
        Ok(())
    }
}

impl ServerClock for GracefulShutdownRuntime {
    fn runtime_now(&self) -> SystemTime {
        runtime_now_wall_clock()
    }
}

#[derive(Debug, Clone, Default)]
pub(in crate::server::tests) struct BlockingShutdownRuntime;

#[async_trait]
impl WorkerTaskRunner for BlockingShutdownRuntime {
    async fn run_task_once(&mut self, _queues: &[String]) -> Result<WorkerRun, ProcessingError> {
        Ok(WorkerRun::NoProcessableTask)
    }
}

#[async_trait]
impl ServerShutdown for BlockingShutdownRuntime {
    async fn shutdown(&mut self) -> Result<(), ProcessingError> {
        std::future::pending::<()>().await;
        Ok(())
    }
}

#[async_trait]
impl ServerConnection for BlockingShutdownRuntime {
    async fn ping(&mut self) -> Result<(), String> {
        connection_ping_noop().await
    }

    fn close(&mut self) -> Result<(), BrokerError> {
        connection_close_noop()
    }
}

impl ServerClock for BlockingShutdownRuntime {
    fn runtime_now(&self) -> SystemTime {
        runtime_now_wall_clock()
    }
}

impl ServerRuntimeStateAttach for BlockingShutdownRuntime {
    fn attach_server_runtime(&mut self, _runtime: &ServerRuntimeState) {}
}

#[async_trait]
impl ServerHeartbeatStore for BlockingShutdownRuntime {
    async fn write_server_metadata(
        &mut self,
        metadata: &ServerMetadata,
    ) -> Result<(), MetadataError> {
        metadata_noop(metadata).await
    }

    async fn clear_server_metadata(
        &mut self,
        metadata: &ServerMetadata,
    ) -> Result<(), MetadataError> {
        metadata_noop(metadata).await
    }
}

#[async_trait]
impl ServerLeaseExtender for BlockingShutdownRuntime {
    async fn extend_leases(
        &mut self,
        queue: &str,
        task_ids: &[String],
    ) -> Result<LeaseExtension, LeaseError> {
        lease_noop(queue, task_ids).await
    }
}

#[async_trait]
impl ServerMaintenanceRunner for BlockingShutdownRuntime {
    async fn run_forwarder_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        maintenance_noop(queues).await
    }

    async fn run_recoverer_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        maintenance_noop(queues).await
    }

    async fn run_janitor_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        maintenance_noop(queues).await
    }
}

#[async_trait]
impl ServerSyncStore for BlockingShutdownRuntime {
    async fn apply_pending_sync_operation(
        &mut self,
        operation: &PendingSyncOperation,
    ) -> Result<(), ()> {
        sync_noop(operation).await
    }
}
