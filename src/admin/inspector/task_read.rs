use crate::{AdminError, ListOption, Pagination, TaskInfo};

use super::{Inspector, validate_inspector_queue};

/// Internal broker operations used by public Inspector task read methods.
///
/// Reference: Asynq v0.26.0 public `Inspector.GetTaskInfo` and task listing
/// methods:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L246-L529>.
#[async_trait::async_trait]
pub trait InspectorTaskReadBroker {
    async fn task_info(&mut self, queue: &str, task_id: &str) -> Result<TaskInfo, AdminError>;
    async fn list_pending_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError>;
    async fn list_active_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError>;
    async fn list_aggregating_tasks(
        &mut self,
        queue: &str,
        group: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError>;
    async fn list_scheduled_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError>;
    async fn list_retry_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError>;
    async fn list_archived_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError>;
    async fn list_completed_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError>;
}

impl<B> Inspector<B>
where
    B: InspectorTaskReadBroker,
{
    /// Returns task metadata by queue and task id.
    ///
    /// Reference: Asynq v0.26.0 public `Inspector.GetTaskInfo` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L246-L263>.
    pub async fn get_task_info(
        &mut self,
        queue: &str,
        task_id: &str,
    ) -> Result<TaskInfo, AdminError> {
        self.broker
            .task_info(queue, task_id)
            .await
            .map_err(AdminError::with_asynq_prefix)
    }

    pub async fn list_pending_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        validate_inspector_queue_for_list(queue)?;
        self.broker
            .list_pending_tasks(queue, pagination)
            .await
            .map_err(AdminError::with_asynq_prefix)
    }

    pub async fn list_pending_tasks_with_options<I>(
        &mut self,
        queue: &str,
        options: I,
    ) -> Result<Vec<TaskInfo>, AdminError>
    where
        I: IntoIterator<Item = ListOption>,
    {
        self.list_pending_tasks(queue, Pagination::from_list_options(options)?)
            .await
    }

    pub async fn list_active_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        validate_inspector_queue_for_list(queue)?;
        self.broker
            .list_active_tasks(queue, pagination)
            .await
            .map_err(AdminError::with_asynq_prefix)
    }

    pub async fn list_active_tasks_with_options<I>(
        &mut self,
        queue: &str,
        options: I,
    ) -> Result<Vec<TaskInfo>, AdminError>
    where
        I: IntoIterator<Item = ListOption>,
    {
        self.list_active_tasks(queue, Pagination::from_list_options(options)?)
            .await
    }

    pub async fn list_aggregating_tasks(
        &mut self,
        queue: &str,
        group: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        validate_inspector_queue_for_list(queue)?;
        self.broker
            .list_aggregating_tasks(queue, group, pagination)
            .await
            .map_err(AdminError::with_asynq_prefix)
    }

    pub async fn list_aggregating_tasks_with_options<I>(
        &mut self,
        queue: &str,
        group: &str,
        options: I,
    ) -> Result<Vec<TaskInfo>, AdminError>
    where
        I: IntoIterator<Item = ListOption>,
    {
        self.list_aggregating_tasks(queue, group, Pagination::from_list_options(options)?)
            .await
    }

    pub async fn list_scheduled_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        validate_inspector_queue_for_list(queue)?;
        self.broker
            .list_scheduled_tasks(queue, pagination)
            .await
            .map_err(AdminError::with_asynq_prefix)
    }

    pub async fn list_scheduled_tasks_with_options<I>(
        &mut self,
        queue: &str,
        options: I,
    ) -> Result<Vec<TaskInfo>, AdminError>
    where
        I: IntoIterator<Item = ListOption>,
    {
        self.list_scheduled_tasks(queue, Pagination::from_list_options(options)?)
            .await
    }

    pub async fn list_retry_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        validate_inspector_queue_for_list(queue)?;
        self.broker
            .list_retry_tasks(queue, pagination)
            .await
            .map_err(AdminError::with_asynq_prefix)
    }

    pub async fn list_retry_tasks_with_options<I>(
        &mut self,
        queue: &str,
        options: I,
    ) -> Result<Vec<TaskInfo>, AdminError>
    where
        I: IntoIterator<Item = ListOption>,
    {
        self.list_retry_tasks(queue, Pagination::from_list_options(options)?)
            .await
    }

    pub async fn list_archived_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        validate_inspector_queue_for_list(queue)?;
        self.broker
            .list_archived_tasks(queue, pagination)
            .await
            .map_err(AdminError::with_asynq_prefix)
    }

    pub async fn list_archived_tasks_with_options<I>(
        &mut self,
        queue: &str,
        options: I,
    ) -> Result<Vec<TaskInfo>, AdminError>
    where
        I: IntoIterator<Item = ListOption>,
    {
        self.list_archived_tasks(queue, Pagination::from_list_options(options)?)
            .await
    }

    pub async fn list_completed_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        validate_inspector_queue_for_list(queue)?;
        self.broker
            .list_completed_tasks(queue, pagination)
            .await
            .map_err(AdminError::with_asynq_prefix)
    }

    pub async fn list_completed_tasks_with_options<I>(
        &mut self,
        queue: &str,
        options: I,
    ) -> Result<Vec<TaskInfo>, AdminError>
    where
        I: IntoIterator<Item = ListOption>,
    {
        self.list_completed_tasks(queue, Pagination::from_list_options(options)?)
            .await
    }
}

fn validate_inspector_queue_for_list(queue: &str) -> Result<(), AdminError> {
    // Reference: Asynq v0.26.0 `Inspector.List*Tasks` wraps queue-name
    // validation errors with an `asynq:` prefix, unlike bulk task methods
    // that return the raw validation error:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L286-L529>.
    validate_inspector_queue(queue).map_err(AdminError::with_asynq_prefix)
}
