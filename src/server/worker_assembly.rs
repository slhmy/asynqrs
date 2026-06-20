//! Server-owned worker dependency assembly.
//!
//! Reference: Asynq v0.26.0 `Server` constructs and owns worker execution
//! goroutines, while processor code consumes the broker/handler/runtime bundle:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L449-L721>.
//!
//! Rust-native note: this type is the owned source for worker hot-path state.
//! Worker construction flows through this server-side assembly boundary rather
//! than an intermediate runtime wrapper.

use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;

use crate::client::Clock;
use crate::client::CloseBroker;
use crate::processing::{
    DefaultIsFailure, DefaultRetryDelay, NoopErrorHandler, NoopLeaseExtender, TaskExecutor,
};
use crate::{BrokerError, LeaseError, LeaseExtension, MetadataError};

use super::{
    BorrowedWorkerParts, DEFAULT_JANITOR_BATCH_SIZE, LeaseBroker, LogLevel, Logger, MetadataBroker,
    PendingSyncBacklog, PendingSyncOperation, PingBroker, ServerClock, ServerConnection,
    ServerHeartbeatStore, ServerLeaseExtender, ServerMetadata, ServerRuntimeState, ServerSyncStore,
    WorkerBrokerCore, WorkerParts,
};

#[derive(Clone)]
pub(crate) struct WorkerAssembly<
    B,
    H,
    R = DefaultRetryDelay,
    C = crate::SystemClock,
    I = DefaultIsFailure,
    E = NoopErrorHandler,
    L = NoopLeaseExtender,
> {
    pub(crate) broker: B,
    pub(crate) executor: TaskExecutor<H, R, I, E, L>,
    pub(crate) clock: C,
    pub(crate) active_task_id: Option<String>,
    pub(crate) runtime: ServerRuntimeState,
    pub(crate) log_level: LogLevel,
    pub(crate) logger: Option<Arc<dyn Logger>>,
    pub(crate) last_dequeue_error_log_at: Option<SystemTime>,
    pub(crate) cleanup_batch_size: isize,
}

impl<B, H, R, C, I, E, L> std::fmt::Debug for WorkerAssembly<B, H, R, C, I, E, L>
where
    B: std::fmt::Debug,
    H: std::fmt::Debug,
    R: std::fmt::Debug,
    C: std::fmt::Debug,
    I: std::fmt::Debug,
    E: std::fmt::Debug,
    L: std::fmt::Debug,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkerAssembly")
            .field("broker", &self.broker)
            .field("executor", &self.executor)
            .field("clock", &self.clock)
            .field("cleanup_batch_size", &self.cleanup_batch_size)
            .field("active_task_id", &self.active_task_id)
            .field("runtime", &self.runtime)
            .field("log_level", &self.log_level)
            .field("has_logger", &self.logger.is_some())
            .field("last_dequeue_error_log_at", &self.last_dequeue_error_log_at)
            .field("pending_sync_count", &self.runtime.pending_sync().len())
            .finish()
    }
}

impl<B, H, R, C, I, E, L> WorkerAssembly<B, H, R, C, I, E, L> {
    pub(crate) fn new(broker: B, executor: TaskExecutor<H, R, I, E, L>, clock: C) -> Self {
        Self {
            broker,
            executor,
            clock,
            active_task_id: None,
            runtime: ServerRuntimeState::default(),
            log_level: LogLevel::Info,
            logger: None,
            last_dequeue_error_log_at: None,
            cleanup_batch_size: DEFAULT_JANITOR_BATCH_SIZE,
        }
    }

    pub(crate) fn set_runtime(&mut self, runtime: &ServerRuntimeState) {
        self.runtime = runtime.clone();
    }

    pub(crate) fn with_optional_logger(mut self, logger: Option<Arc<dyn Logger>>) -> Self {
        self.logger = logger;
        self
    }

    pub(crate) fn with_log_level(mut self, log_level: LogLevel) -> Self {
        self.log_level = match log_level {
            LogLevel::Unspecified => LogLevel::Info,
            log_level => log_level,
        };
        self
    }

    pub(crate) fn with_cleanup_batch_size(mut self, batch_size: isize) -> Self {
        self.cleanup_batch_size = if batch_size == 0 {
            DEFAULT_JANITOR_BATCH_SIZE
        } else {
            batch_size
        };
        self
    }

    pub(crate) fn worker_parts(&self) -> WorkerParts<B, H, R, C, I, E, L>
    where
        B: Clone,
        TaskExecutor<H, R, I, E, L>: Clone,
        C: Clone,
    {
        WorkerParts::new(
            self.broker.clone(),
            self.executor.clone(),
            self.clock.clone(),
            self.runtime.clone(),
            self.logger.clone(),
            self.log_level,
        )
    }

    pub(crate) fn borrowed_worker_parts(&mut self) -> BorrowedWorkerParts<'_, B, H, R, C, I, E, L> {
        BorrowedWorkerParts {
            broker: &mut self.broker,
            executor: &mut self.executor,
            clock: &self.clock,
            active_task_id: &mut self.active_task_id,
            runtime: &self.runtime,
            log_level: self.log_level,
            logger: &self.logger,
            last_dequeue_error_log_at: &mut self.last_dequeue_error_log_at,
        }
    }
}

#[async_trait]
impl<B, H, R, C, I, E, L> ServerConnection for WorkerAssembly<B, H, R, C, I, E, L>
where
    B: PingBroker + CloseBroker + Send,
    H: Send,
    R: Send,
    C: Send + Sync,
    I: Send,
    E: Send,
    L: Send,
{
    async fn ping(&mut self) -> Result<(), String> {
        self.broker.ping().await
    }

    fn close(&mut self) -> Result<(), BrokerError> {
        self.broker.close()
    }
}

impl<B, H, R, C, I, E, L> ServerClock for WorkerAssembly<B, H, R, C, I, E, L>
where
    B: Send,
    H: Send,
    R: Send,
    C: Clock + Send + Sync,
    I: Send,
    E: Send,
    L: Send,
{
    fn runtime_now(&self) -> SystemTime {
        self.clock.now()
    }
}

#[async_trait]
impl<B, H, R, C, I, E, L> ServerLeaseExtender for WorkerAssembly<B, H, R, C, I, E, L>
where
    B: LeaseBroker + Send,
    H: Send,
    R: Send,
    C: Send + Sync,
    I: Send,
    E: Send,
    L: Send,
{
    async fn extend_leases(
        &mut self,
        queue: &str,
        task_ids: &[String],
    ) -> Result<LeaseExtension, LeaseError> {
        self.broker.extend_leases(queue, task_ids).await
    }
}

#[async_trait]
impl<B, H, R, C, I, E, L> ServerHeartbeatStore for WorkerAssembly<B, H, R, C, I, E, L>
where
    B: MetadataBroker + Send,
    H: Send,
    R: Send,
    C: Send + Sync,
    I: Send,
    E: Send,
    L: Send,
{
    async fn write_server_metadata(
        &mut self,
        metadata: &ServerMetadata,
    ) -> Result<(), MetadataError> {
        self.broker
            .write_server_state(
                metadata.hostname(),
                metadata.pid(),
                metadata.server_id(),
                metadata.server_info().to_vec(),
                metadata.workers().to_vec(),
                metadata.ttl(),
            )
            .await
    }

    async fn clear_server_metadata(
        &mut self,
        metadata: &ServerMetadata,
    ) -> Result<(), MetadataError> {
        self.broker
            .clear_server_state(metadata.hostname(), metadata.pid(), metadata.server_id())
            .await
    }
}

#[async_trait]
impl<B, H, R, C, I, E, L> ServerSyncStore for WorkerAssembly<B, H, R, C, I, E, L>
where
    B: WorkerBrokerCore + Send,
    H: Send,
    R: Send,
    C: Send + Sync,
    I: Send,
    E: Send,
    L: Send,
{
    async fn apply_pending_sync_operation(
        &mut self,
        operation: &PendingSyncOperation,
    ) -> Result<(), ()> {
        PendingSyncBacklog::apply_operation(&mut self.broker, operation).await
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use async_trait::async_trait;

    use crate::client::Clock;
    use crate::processing::{DefaultIsFailure, DefaultRetryDelay};
    use crate::{BrokerError, Task};

    use super::*;

    type MetadataWrite = (String, i32, String, Vec<u8>, Vec<Vec<u8>>, Duration);
    type RecordingAssembly = WorkerAssembly<
        RecordingLifecycleBroker,
        fn(&Task) -> Result<(), crate::HandlerError>,
        DefaultRetryDelay,
        FixedClock,
        DefaultIsFailure,
        crate::NoopErrorHandler,
        crate::NoopLeaseExtender,
    >;

    #[derive(Default)]
    struct RecordingLifecycleBroker {
        pings: usize,
        closes: usize,
        metadata_writes: Vec<MetadataWrite>,
        metadata_clears: Vec<(String, i32, String)>,
        lease_extensions: Vec<(String, Vec<String>)>,
        lease_expires_at: Option<SystemTime>,
    }

    #[async_trait]
    impl PingBroker for RecordingLifecycleBroker {
        async fn ping(&mut self) -> Result<(), String> {
            self.pings += 1;
            Ok(())
        }
    }

    impl CloseBroker for RecordingLifecycleBroker {
        fn close(&mut self) -> Result<(), BrokerError> {
            self.closes += 1;
            Ok(())
        }
    }

    #[async_trait]
    impl MetadataBroker for RecordingLifecycleBroker {
        async fn write_server_state(
            &mut self,
            hostname: &str,
            pid: i32,
            server_id: &str,
            server_info: Vec<u8>,
            workers: Vec<Vec<u8>>,
            ttl: Duration,
        ) -> Result<(), MetadataError> {
            self.metadata_writes.push((
                hostname.to_owned(),
                pid,
                server_id.to_owned(),
                server_info,
                workers,
                ttl,
            ));
            Ok(())
        }

        async fn clear_server_state(
            &mut self,
            hostname: &str,
            pid: i32,
            server_id: &str,
        ) -> Result<(), MetadataError> {
            self.metadata_clears
                .push((hostname.to_owned(), pid, server_id.to_owned()));
            Ok(())
        }
    }

    #[async_trait]
    impl LeaseBroker for RecordingLifecycleBroker {
        async fn extend_leases(
            &mut self,
            queue: &str,
            task_ids: &[String],
        ) -> Result<LeaseExtension, LeaseError> {
            self.lease_extensions
                .push((queue.to_owned(), task_ids.to_vec()));
            Ok(LeaseExtension::new(
                self.lease_expires_at.unwrap_or(SystemTime::UNIX_EPOCH),
            ))
        }
    }

    #[derive(Clone, Copy)]
    struct FixedClock(SystemTime);

    impl Clock for FixedClock {
        fn now(&self) -> SystemTime {
            self.0
        }
    }

    fn assembly(now: SystemTime) -> RecordingAssembly {
        fn handler(_task: &Task) -> Result<(), crate::HandlerError> {
            Ok(())
        }

        let executor = TaskExecutor::new(
            handler as fn(&Task) -> _,
            DefaultRetryDelay,
            DefaultIsFailure,
        );
        WorkerAssembly::new(
            RecordingLifecycleBroker::default(),
            executor,
            FixedClock(now),
        )
    }

    #[tokio::test]
    async fn worker_assembly_owns_connection_clock_metadata_and_lease_capabilities() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(42);
        let lease_expires_at = now + Duration::from_secs(30);
        let mut assembly = assembly(now);
        assembly.broker.lease_expires_at = Some(lease_expires_at);

        ServerConnection::ping(&mut assembly).await.unwrap();
        ServerConnection::close(&mut assembly).unwrap();
        assert_eq!(assembly.broker.pings, 1);
        assert_eq!(assembly.broker.closes, 1);
        assert_eq!(ServerClock::runtime_now(&assembly), now);

        let metadata = ServerMetadata::new(
            "host",
            123,
            "server-id",
            b"server-info".to_vec(),
            ["worker-info"],
            Duration::from_secs(5),
        )
        .unwrap();
        ServerHeartbeatStore::write_server_metadata(&mut assembly, &metadata)
            .await
            .unwrap();
        ServerHeartbeatStore::clear_server_metadata(&mut assembly, &metadata)
            .await
            .unwrap();

        assert_eq!(assembly.broker.metadata_writes.len(), 1);
        assert_eq!(assembly.broker.metadata_writes[0].0, "host");
        assert_eq!(
            assembly.broker.metadata_clears,
            vec![("host".to_owned(), 123, "server-id".to_owned())]
        );

        let extension = ServerLeaseExtender::extend_leases(
            &mut assembly,
            "critical",
            &["task-a".to_owned(), "task-b".to_owned()],
        )
        .await
        .unwrap();
        assert_eq!(extension.expires_at(), lease_expires_at);
        assert_eq!(
            assembly.broker.lease_extensions,
            vec![(
                "critical".to_owned(),
                vec!["task-a".to_owned(), "task-b".to_owned()]
            )]
        );
    }
}
