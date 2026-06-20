//! Scheduler broker boundary.
//!
//! Reference: Asynq v0.26.0 scheduler delegates enqueue, heartbeat metadata,
//! enqueue history, and cleanup operations to the Redis broker:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L154-L375>.

use std::time::{Duration, SystemTime};

use async_trait::async_trait;

use crate::{BrokerError, EnqueuePlan, MetadataError};

/// Storage and metadata boundary used by generic schedulers.
///
/// Most applications should construct `RedisBackedScheduler` instead of
/// implementing this trait. Implement it only when embedding `Scheduler<B, C>`
/// or `PeriodicTaskManager<P, B, C>` with a custom scheduling backend.
///
/// Reference: Asynq v0.26.0 scheduler delegates enqueue, heartbeat metadata,
/// enqueue history, and cleanup operations to Redis-backed storage:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L154-L375>.
#[async_trait]
pub trait SchedulerBroker {
    async fn ping(&mut self) -> Result<(), String>;

    /// Closes broker resources owned by the running scheduler.
    ///
    /// Reference: Asynq v0.26.0 `Scheduler.Shutdown` closes its Redis broker
    /// after stopping scheduler lifecycle work:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L309-L334>.
    fn close(&mut self);

    async fn enqueue_scheduled(&mut self, plan: &EnqueuePlan) -> Result<(), BrokerError>;

    async fn write_scheduler_entries(
        &mut self,
        scheduler_id: &str,
        entries: Vec<(String, Vec<u8>)>,
        ttl: Duration,
    ) -> Result<(), MetadataError>;

    async fn record_scheduler_enqueue_event(
        &mut self,
        entry_id: &str,
        event: Vec<u8>,
        now: SystemTime,
    ) -> Result<(), MetadataError>;

    async fn clear_scheduler_entries(&mut self, scheduler_id: &str) -> Result<(), MetadataError>;

    async fn clear_scheduler_history(&mut self, entry_id: &str) -> Result<(), MetadataError>;
}
