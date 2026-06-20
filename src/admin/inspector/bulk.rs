use crate::AdminError;

use super::{Inspector, validate_inspector_queue};

/// Internal broker operations used by public Inspector bulk task mutation
/// methods.
///
/// Reference: Asynq v0.26.0 public Inspector `RunAll*`, `ArchiveAll*`, and
/// `DeleteAll*` methods:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L531-L614>.
#[async_trait::async_trait]
pub trait InspectorBulkBroker {
    async fn run_all_scheduled_tasks(&mut self, queue: &str) -> Result<usize, AdminError>;
    async fn run_all_retry_tasks(&mut self, queue: &str) -> Result<usize, AdminError>;
    async fn run_all_archived_tasks(&mut self, queue: &str) -> Result<usize, AdminError>;
    async fn run_all_aggregating_tasks(
        &mut self,
        queue: &str,
        group: &str,
    ) -> Result<usize, AdminError>;
    async fn archive_all_pending_tasks(&mut self, queue: &str) -> Result<usize, AdminError>;
    async fn archive_all_scheduled_tasks(&mut self, queue: &str) -> Result<usize, AdminError>;
    async fn archive_all_retry_tasks(&mut self, queue: &str) -> Result<usize, AdminError>;
    async fn archive_all_aggregating_tasks(
        &mut self,
        queue: &str,
        group: &str,
    ) -> Result<usize, AdminError>;
    async fn delete_all_pending_tasks(&mut self, queue: &str) -> Result<usize, AdminError>;
    async fn delete_all_scheduled_tasks(&mut self, queue: &str) -> Result<usize, AdminError>;
    async fn delete_all_retry_tasks(&mut self, queue: &str) -> Result<usize, AdminError>;
    async fn delete_all_archived_tasks(&mut self, queue: &str) -> Result<usize, AdminError>;
    async fn delete_all_completed_tasks(&mut self, queue: &str) -> Result<usize, AdminError>;
    async fn delete_all_aggregating_tasks(
        &mut self,
        queue: &str,
        group: &str,
    ) -> Result<usize, AdminError>;
}

impl<B> Inspector<B>
where
    B: InspectorBulkBroker,
{
    /// Reference: Asynq v0.26.0 public `Inspector.RunAllScheduledTasks` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L531-L540>.
    pub async fn run_all_scheduled_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        validate_inspector_queue(queue)?;
        self.broker.run_all_scheduled_tasks(queue).await
    }

    /// Reference: Asynq v0.26.0 public `Inspector.RunAllRetryTasks` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L542-L551>.
    pub async fn run_all_retry_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        validate_inspector_queue(queue)?;
        self.broker.run_all_retry_tasks(queue).await
    }

    /// Reference: Asynq v0.26.0 public `Inspector.RunAllArchivedTasks` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L553-L562>.
    pub async fn run_all_archived_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        validate_inspector_queue(queue)?;
        self.broker.run_all_archived_tasks(queue).await
    }

    /// Reference: Asynq v0.26.0 public `Inspector.RunAllAggregatingTasks` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L564-L573>.
    pub async fn run_all_aggregating_tasks(
        &mut self,
        queue: &str,
        group: &str,
    ) -> Result<usize, AdminError> {
        validate_inspector_queue(queue)?;
        self.broker.run_all_aggregating_tasks(queue, group).await
    }

    /// Reference: Asynq v0.26.0 public `Inspector.ArchiveAllPendingTasks` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L575-L584>.
    pub async fn archive_all_pending_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        validate_inspector_queue(queue)?;
        self.broker.archive_all_pending_tasks(queue).await
    }

    /// Reference: Asynq v0.26.0 public `Inspector.ArchiveAllScheduledTasks`
    /// method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L586-L595>.
    pub async fn archive_all_scheduled_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        validate_inspector_queue(queue)?;
        self.broker.archive_all_scheduled_tasks(queue).await
    }

    /// Reference: Asynq v0.26.0 public `Inspector.ArchiveAllRetryTasks` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L597-L606>.
    pub async fn archive_all_retry_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        validate_inspector_queue(queue)?;
        self.broker.archive_all_retry_tasks(queue).await
    }

    /// Reference: Asynq v0.26.0 public `Inspector.ArchiveAllAggregatingTasks`
    /// method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L608-L614>.
    pub async fn archive_all_aggregating_tasks(
        &mut self,
        queue: &str,
        group: &str,
    ) -> Result<usize, AdminError> {
        validate_inspector_queue(queue)?;
        self.broker
            .archive_all_aggregating_tasks(queue, group)
            .await
    }

    /// Reference: Asynq v0.26.0 public `Inspector.DeleteAllPendingTasks` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L641-L650>.
    pub async fn delete_all_pending_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        validate_inspector_queue(queue)?;
        self.broker.delete_all_pending_tasks(queue).await
    }

    /// Reference: Asynq v0.26.0 public `Inspector.DeleteAllScheduledTasks` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L652-L661>.
    pub async fn delete_all_scheduled_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        validate_inspector_queue(queue)?;
        self.broker.delete_all_scheduled_tasks(queue).await
    }

    /// Reference: Asynq v0.26.0 public `Inspector.DeleteAllRetryTasks` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L663-L672>.
    pub async fn delete_all_retry_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        validate_inspector_queue(queue)?;
        self.broker.delete_all_retry_tasks(queue).await
    }

    /// Reference: Asynq v0.26.0 public `Inspector.DeleteAllArchivedTasks` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L674-L683>.
    pub async fn delete_all_archived_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        validate_inspector_queue(queue)?;
        self.broker.delete_all_archived_tasks(queue).await
    }

    /// Reference: Asynq v0.26.0 public `Inspector.DeleteAllCompletedTasks` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L685-L694>.
    pub async fn delete_all_completed_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        validate_inspector_queue(queue)?;
        self.broker.delete_all_completed_tasks(queue).await
    }

    /// Reference: Asynq v0.26.0 public `Inspector.DeleteAllAggregatingTasks`
    /// method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L726-L729>.
    pub async fn delete_all_aggregating_tasks(
        &mut self,
        queue: &str,
        group: &str,
    ) -> Result<usize, AdminError> {
        validate_inspector_queue(queue)?;
        self.broker.delete_all_aggregating_tasks(queue, group).await
    }
}
