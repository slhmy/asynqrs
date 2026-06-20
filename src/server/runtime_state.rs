//! Server-owned runtime state.
//!
//! Reference: Asynq v0.26.0 `Server` owns runtime lifecycle while processor,
//! heartbeat, syncer, and subscriber components share active worker,
//! cancellation, and sync state:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/server.go>,
//! <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go>,
//! <https://github.com/hibiken/asynq/blob/v0.26.0/heartbeat.go>, and
//! <https://github.com/hibiken/asynq/blob/v0.26.0/syncer.go>.

use std::sync::Arc;
use std::time::SystemTime;

use tokio_util::sync::CancellationToken;

use crate::task::TaskMessage;
use crate::{LeaseError, LeaseExtension, ServerCanceller, ServerMetadata};

use super::logging::log_warn;
use super::{
    ActiveWorkerRegistry, ActiveWorkerSnapshot, LogLevel, Logger, PendingSyncBacklog,
    PendingSyncDrainPolicy, ProcessingLease, ServerLeaseExtender, ServerSyncStore,
};

/// Shared runtime state owned by a server instance.
///
/// Rust-native note: this is the replacement center for the old
/// processor-era runtime handle. Workers, heartbeat, syncer, shutdown, and
/// cancellation all use this server-owned object rather than an
/// implementation-shaped processing runtime owner.
#[derive(Debug, Clone, Default)]
pub(crate) struct ServerRuntimeState {
    active_workers: ActiveWorkerRegistry,
    canceller: ServerCanceller,
    pending_sync: PendingSyncBacklog,
}

#[derive(Debug, Clone)]
pub(crate) struct ActiveLeaseBatch {
    queue: String,
    task_ids: Vec<String>,
    leases: Vec<ProcessingLease>,
}

impl ServerRuntimeState {
    pub(crate) fn canceller(&self) -> ServerCanceller {
        self.canceller.clone()
    }

    pub(crate) fn pending_sync(&self) -> PendingSyncBacklog {
        self.pending_sync.clone()
    }

    pub(crate) fn register_cancellation(&self, task_id: &str) -> CancellationToken {
        self.canceller.register(task_id)
    }

    pub(crate) fn unregister_cancellation(&self, task_id: &str) {
        self.canceller.unregister(task_id);
    }

    pub(crate) fn insert_active_worker(
        &self,
        message: TaskMessage,
        started_at: SystemTime,
        deadline: Option<SystemTime>,
        lease: ProcessingLease,
    ) {
        self.active_workers
            .insert(message, started_at, deadline, lease);
    }

    pub(crate) fn remove_active_worker(&self, task_id: &str) -> Option<ActiveWorkerSnapshot> {
        self.active_workers.remove(task_id)
    }

    pub(crate) fn server_metadata_snapshot(&self, metadata: &ServerMetadata) -> ServerMetadata {
        if self.active_workers.is_empty() {
            return metadata.clone();
        }
        self.active_workers.server_metadata_snapshot(metadata)
    }

    pub(crate) fn active_lease_batches(&self, now: SystemTime) -> Vec<ActiveLeaseBatch> {
        self.active_workers
            .active_leases_by_queue(now)
            .into_iter()
            .map(|(queue, tasks)| {
                let (task_ids, leases) = tasks.into_iter().unzip();
                ActiveLeaseBatch {
                    queue,
                    task_ids,
                    leases,
                }
            })
            .collect()
    }

    pub(in crate::server) async fn extend_active_worker_leases<P>(
        &self,
        lease_extender: &mut P,
        now: SystemTime,
        on_error: impl Fn(&[String], &LeaseError),
    ) where
        P: ServerLeaseExtender + Send,
    {
        for batch in self.active_lease_batches(now) {
            match lease_extender
                .extend_leases(batch.queue(), batch.task_ids())
                .await
            {
                Ok(extension) => batch.reset_leases(extension),
                Err(error) => on_error(batch.task_ids(), &error),
            }
        }
    }

    pub(in crate::server) async fn drain_pending_sync<P>(
        &self,
        sync_store: &mut P,
        policy: PendingSyncDrainPolicy,
        logger: &Option<Arc<dyn Logger>>,
        log_level: LogLevel,
        now: SystemTime,
    ) where
        P: ServerSyncStore + Send,
    {
        let pending_sync = self.pending_sync();
        let pending = pending_sync.len();
        for _ in 0..pending {
            let Some(operation) = pending_sync.pop() else {
                break;
            };
            if policy.drop_stale() && operation.deadline() < now {
                continue;
            }
            if sync_store
                .apply_pending_sync_operation(&operation)
                .await
                .is_err()
            {
                if policy.log_final_failure() {
                    log_warn(
                        logger,
                        log_level,
                        format_args!("{}", operation.sync_error_message()),
                    );
                }
                if policy.requeue_failures() {
                    pending_sync.push(operation);
                }
            }
        }
    }
}

impl ActiveLeaseBatch {
    pub(crate) fn queue(&self) -> &str {
        &self.queue
    }

    pub(crate) fn task_ids(&self) -> &[String] {
        &self.task_ids
    }

    pub(crate) fn reset_leases(self, extension: LeaseExtension) {
        for lease in self.leases {
            lease.reset(extension);
        }
    }
}
