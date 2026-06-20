use crate::{
    AdminError, ClusterNode, ListOption, Pagination, SchedulerEnqueueEventInfo, SchedulerEntryInfo,
    ServerInfo,
};

use super::Inspector;

/// Internal broker operations used by public Inspector runtime and scheduler
/// metadata methods.
///
/// Reference: Asynq v0.26.0 public `Inspector.Servers`,
/// `Inspector.ClusterKeySlot`, `Inspector.ClusterNodes`,
/// `Inspector.SchedulerEntries`, and `Inspector.ListSchedulerEnqueueEvents`
/// methods:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L790-L1051>.
#[async_trait::async_trait]
pub trait InspectorMetadataBroker {
    async fn servers(&mut self) -> Result<Vec<ServerInfo>, AdminError>;
    async fn cluster_key_slot(&mut self, queue: &str) -> Result<i64, AdminError>;
    async fn cluster_nodes(&mut self, queue: &str) -> Result<Vec<ClusterNode>, AdminError>;
    async fn scheduler_entries(&mut self) -> Result<Vec<SchedulerEntryInfo>, AdminError>;
    async fn list_scheduler_enqueue_events(
        &mut self,
        entry_id: &str,
        pagination: Pagination,
    ) -> Result<Vec<SchedulerEnqueueEventInfo>, AdminError>;
}

impl<B> Inspector<B>
where
    B: InspectorMetadataBroker,
{
    /// Returns currently running server metadata and active workers.
    ///
    /// Reference: Asynq v0.26.0 public `Inspector.Servers` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L790-L826>.
    pub async fn servers(&mut self) -> Result<Vec<ServerInfo>, AdminError> {
        self.broker.servers().await
    }

    /// Returns the Redis cluster hash slot used by the queue pending key.
    ///
    /// Reference: Asynq v0.26.0 public `Inspector.ClusterKeySlot` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L852-L854>.
    pub async fn cluster_key_slot(&mut self, queue: &str) -> Result<i64, AdminError> {
        self.broker.cluster_key_slot(queue).await
    }

    /// Returns Redis cluster nodes that own the queue pending key slot.
    ///
    /// Reference: Asynq v0.26.0 public `Inspector.ClusterNodes` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L856-L877>.
    pub async fn cluster_nodes(&mut self, queue: &str) -> Result<Vec<ClusterNode>, AdminError> {
        self.broker.cluster_nodes(queue).await
    }

    /// Returns scheduler entries registered by running schedulers.
    ///
    /// Reference: Asynq v0.26.0 public `Inspector.SchedulerEntries` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L923-L944>.
    pub async fn scheduler_entries(&mut self) -> Result<Vec<SchedulerEntryInfo>, AdminError> {
        self.broker.scheduler_entries().await
    }

    /// Returns recent enqueue events for a scheduler entry.
    ///
    /// Reference: Asynq v0.26.0 public
    /// `Inspector.ListSchedulerEnqueueEvents` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L1036-L1051>.
    pub async fn list_scheduler_enqueue_events(
        &mut self,
        entry_id: &str,
        pagination: Pagination,
    ) -> Result<Vec<SchedulerEnqueueEventInfo>, AdminError> {
        self.broker
            .list_scheduler_enqueue_events(entry_id, pagination)
            .await
    }

    /// Reference: Asynq v0.26.0 public
    /// `Inspector.ListSchedulerEnqueueEvents` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L1036-L1051>.
    pub async fn list_scheduler_enqueue_events_with_options<I>(
        &mut self,
        entry_id: &str,
        options: I,
    ) -> Result<Vec<SchedulerEnqueueEventInfo>, AdminError>
    where
        I: IntoIterator<Item = ListOption>,
    {
        self.list_scheduler_enqueue_events(entry_id, Pagination::from_list_options(options)?)
            .await
    }
}
