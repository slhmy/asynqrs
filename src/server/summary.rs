use super::WorkerRun;

/// Summary of work performed by a server run.
///
/// Reference: Asynq v0.26.0 `Server.Run` coordinates workers and lifecycle
/// components but does not expose a direct summary type:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L653-L721>.
///
/// Rust compatibility: this summary remains crate-local runtime observability
/// while public `Run`/`Start` behavior stays upstream-shaped.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServerRunSummary {
    pub(crate) processed: usize,
    pub(crate) completed: usize,
    pub(crate) retried: usize,
    pub(crate) lease_expired: usize,
    pub(crate) archived: usize,
    pub(crate) revoked: usize,
    pub(crate) idle_polls: usize,
    pub(crate) forwarded_scheduled: usize,
    pub(crate) forwarded_retry: usize,
    pub(crate) recovered_retried: usize,
    pub(crate) recovered_archived: usize,
    pub(crate) deleted_expired_completed: usize,
}

/// Summary of periodic server maintenance work.
///
/// Reference: Asynq v0.26.0 forwarder, recoverer, and janitor components:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/forwarder.go>,
/// <https://github.com/hibiken/asynq/blob/v0.26.0/recoverer.go>,
/// and <https://github.com/hibiken/asynq/blob/v0.26.0/janitor.go>.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ServerMaintenanceRun {
    pub(crate) forwarded_scheduled: usize,
    pub(crate) forwarded_retry: usize,
    pub(crate) recovered_retried: usize,
    pub(crate) recovered_archived: usize,
    pub(crate) deleted_expired_completed: usize,
}

impl ServerRunSummary {
    pub fn processed(&self) -> usize {
        self.processed
    }

    pub fn completed(&self) -> usize {
        self.completed
    }

    pub fn retried(&self) -> usize {
        self.retried
    }

    pub fn lease_expired(&self) -> usize {
        self.lease_expired
    }

    pub fn archived(&self) -> usize {
        self.archived
    }

    pub fn revoked(&self) -> usize {
        self.revoked
    }

    pub fn idle_polls(&self) -> usize {
        self.idle_polls
    }

    pub fn forwarded_scheduled(&self) -> usize {
        self.forwarded_scheduled
    }

    pub fn forwarded_retry(&self) -> usize {
        self.forwarded_retry
    }

    pub fn recovered_retried(&self) -> usize {
        self.recovered_retried
    }

    pub fn recovered_archived(&self) -> usize {
        self.recovered_archived
    }

    pub fn deleted_expired_completed(&self) -> usize {
        self.deleted_expired_completed
    }

    pub(crate) fn record(&mut self, result: WorkerRun) {
        self.processed += 1;
        match result {
            WorkerRun::Completed { .. } => self.completed += 1,
            WorkerRun::Retried { .. } => self.retried += 1,
            WorkerRun::LeaseExpired { .. } => self.lease_expired += 1,
            WorkerRun::Archived { .. } => self.archived += 1,
            WorkerRun::Revoked { .. } => self.revoked += 1,
            WorkerRun::NoProcessableTask => self.idle_polls += 1,
        }
    }

    pub(crate) fn record_idle_poll(&mut self) {
        self.idle_polls += 1;
    }

    pub(crate) fn record_maintenance(&mut self, result: ServerMaintenanceRun) {
        self.forwarded_scheduled += result.forwarded_scheduled;
        self.forwarded_retry += result.forwarded_retry;
        self.recovered_retried += result.recovered_retried;
        self.recovered_archived += result.recovered_archived;
        self.deleted_expired_completed += result.deleted_expired_completed;
    }

    pub(crate) fn merge(&mut self, other: ServerRunSummary) {
        self.processed += other.processed;
        self.completed += other.completed;
        self.retried += other.retried;
        self.lease_expired += other.lease_expired;
        self.archived += other.archived;
        self.revoked += other.revoked;
        self.idle_polls += other.idle_polls;
        self.forwarded_scheduled += other.forwarded_scheduled;
        self.forwarded_retry += other.forwarded_retry;
        self.recovered_retried += other.recovered_retried;
        self.recovered_archived += other.recovered_archived;
        self.deleted_expired_completed += other.deleted_expired_completed;
    }
}

impl ServerMaintenanceRun {
    pub(crate) fn new(
        forwarded_scheduled: usize,
        forwarded_retry: usize,
        recovered_retried: usize,
        recovered_archived: usize,
        deleted_expired_completed: usize,
    ) -> Self {
        Self {
            forwarded_scheduled,
            forwarded_retry,
            recovered_retried,
            recovered_archived,
            deleted_expired_completed,
        }
    }

    pub(crate) fn forwarded_scheduled(&self) -> usize {
        self.forwarded_scheduled
    }

    pub(crate) fn forwarded_retry(&self) -> usize {
        self.forwarded_retry
    }

    pub(crate) fn merge(&mut self, other: ServerMaintenanceRun) {
        self.forwarded_scheduled += other.forwarded_scheduled;
        self.forwarded_retry += other.forwarded_retry;
        self.recovered_retried += other.recovered_retried;
        self.recovered_archived += other.recovered_archived;
        self.deleted_expired_completed += other.deleted_expired_completed;
    }
}
