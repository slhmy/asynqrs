use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use crate::ServerMetadata;
use crate::task::TaskMessage;

use super::{ProcessingLease, worker_info_bytes};

#[derive(Debug, Clone)]
pub(crate) struct ActiveWorkerSnapshot {
    pub(crate) message: TaskMessage,
    pub(crate) started_at: SystemTime,
    pub(crate) deadline: Option<SystemTime>,
    pub(crate) lease: ProcessingLease,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ActiveWorkerRegistry {
    workers: Arc<Mutex<HashMap<String, ActiveWorkerSnapshot>>>,
}

impl ActiveWorkerRegistry {
    pub(crate) fn insert(
        &self,
        message: TaskMessage,
        started_at: SystemTime,
        deadline: Option<SystemTime>,
        lease: ProcessingLease,
    ) {
        self.workers
            .lock()
            .expect("server active worker registry poisoned")
            .insert(
                message.id.clone(),
                ActiveWorkerSnapshot {
                    message,
                    started_at,
                    deadline,
                    lease,
                },
            );
    }

    pub(crate) fn remove(&self, task_id: &str) -> Option<ActiveWorkerSnapshot> {
        self.workers
            .lock()
            .expect("server active worker registry poisoned")
            .remove(task_id)
    }

    fn snapshots(&self) -> Vec<ActiveWorkerSnapshot> {
        let mut snapshots: Vec<_> = self
            .workers
            .lock()
            .expect("server active worker registry poisoned")
            .values()
            .cloned()
            .collect();
        snapshots.sort_by(|left, right| left.message.id.cmp(&right.message.id));
        snapshots
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.workers
            .lock()
            .expect("server active worker registry poisoned")
            .is_empty()
    }

    pub(crate) fn server_metadata_snapshot(&self, metadata: &ServerMetadata) -> ServerMetadata {
        // Reference: Asynq v0.26.0 heartbeater reads all currently active
        // workers from a shared worker map before writing server state:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/heartbeat.go#L129-L154>.
        let workers = self
            .snapshots()
            .into_iter()
            .map(|worker| {
                worker_info_bytes(
                    metadata.hostname(),
                    metadata.pid(),
                    metadata.server_id(),
                    &worker.message,
                    worker.started_at,
                    worker.deadline,
                )
            })
            .collect();
        metadata.with_active_workers(workers)
    }

    pub(crate) fn active_leases(&self, now: SystemTime) -> Vec<(String, String, ProcessingLease)> {
        // Reference: Asynq v0.26.0 heartbeater extends only active workers
        // whose lease is still valid, leaving expired leases for cancellation
        // notification/recoverer ownership:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/heartbeat.go#L170-L201>.
        self.snapshots()
            .into_iter()
            .filter(|worker| worker.lease.is_valid_at(now))
            .map(|worker| (worker.message.queue, worker.message.id, worker.lease))
            .collect()
    }

    pub(crate) fn active_leases_by_queue(
        &self,
        now: SystemTime,
    ) -> Vec<(String, Vec<(String, ProcessingLease)>)> {
        // Reference: Asynq v0.26.0 heartbeater batches lease extension by
        // queue before calling `ExtendLease(qname, ids...)`:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/heartbeat.go#L170-L201>.
        let mut grouped = HashMap::<String, Vec<(String, ProcessingLease)>>::new();
        for (queue, task_id, lease) in self.active_leases(now) {
            grouped.entry(queue).or_default().push((task_id, lease));
        }
        let mut grouped = grouped.into_iter().collect::<Vec<_>>();
        grouped.sort_by(|(left, _), (right, _)| left.cmp(right));
        for (_, tasks) in &mut grouped {
            tasks.sort_by(|(left, _), (right, _)| left.cmp(right));
        }
        grouped
    }
}
