use crate::admin::{
    InspectorBulkBroker, InspectorMetadataBroker, InspectorQueueBroker, InspectorStatsBroker,
    InspectorTaskBroker, InspectorTaskReadBroker,
};
use crate::broker::redis::RedisExecutor;
use crate::client::Clock;
use crate::{
    AdminError, ClusterNode, DailyStats, GroupStats, Pagination, QueueStats,
    SchedulerEnqueueEventInfo, SchedulerEntryInfo, ServerInfo, TaskInfo,
};

use super::super::RedisBroker;

#[async_trait::async_trait]
impl<E, C> InspectorQueueBroker for RedisBroker<E, C>
where
    E: RedisExecutor + Send,
    C: Clock + Send + Sync,
{
    async fn pause_queue(&mut self, queue: &str) -> Result<(), AdminError> {
        RedisBroker::pause_queue(self, queue).await
    }

    async fn unpause_queue(&mut self, queue: &str) -> Result<(), AdminError> {
        RedisBroker::unpause_queue(self, queue).await
    }

    async fn delete_queue(&mut self, queue: &str, force: bool) -> Result<(), AdminError> {
        if force {
            RedisBroker::delete_queue_force(self, queue).await
        } else {
            RedisBroker::delete_queue(self, queue).await
        }
    }
}

#[async_trait::async_trait]
impl<E, C> InspectorTaskBroker for RedisBroker<E, C>
where
    E: RedisExecutor + Send,
    C: Clock + Send + Sync,
{
    async fn run_task(&mut self, queue: &str, task_id: &str) -> Result<(), AdminError> {
        RedisBroker::run_task(self, queue, task_id).await
    }

    async fn archive_task(&mut self, queue: &str, task_id: &str) -> Result<(), AdminError> {
        RedisBroker::archive_task(self, queue, task_id).await
    }

    async fn delete_task(&mut self, queue: &str, task_id: &str) -> Result<(), AdminError> {
        RedisBroker::delete_task(self, queue, task_id).await
    }

    async fn update_task_payload(
        &mut self,
        queue: &str,
        task_id: &str,
        payload: Vec<u8>,
    ) -> Result<(), AdminError> {
        RedisBroker::update_task_payload(self, queue, task_id, payload).await
    }
}

#[async_trait::async_trait]
impl<E, C> InspectorStatsBroker for RedisBroker<E, C>
where
    E: RedisExecutor + Send,
    C: Clock + Send + Sync,
{
    async fn list_queues(&mut self) -> Result<Vec<String>, AdminError> {
        RedisBroker::list_queues(self).await
    }

    async fn group_stats(&mut self, queue: &str) -> Result<Vec<GroupStats>, AdminError> {
        RedisBroker::group_stats(self, queue).await
    }

    async fn current_queue_stats(&mut self, queue: &str) -> Result<QueueStats, AdminError> {
        RedisBroker::current_queue_stats(self, queue).await
    }

    async fn historical_queue_stats(
        &mut self,
        queue: &str,
        days: isize,
    ) -> Result<Vec<DailyStats>, AdminError> {
        RedisBroker::historical_queue_stats(self, queue, days).await
    }
}

#[async_trait::async_trait]
impl<E, C> InspectorBulkBroker for RedisBroker<E, C>
where
    E: RedisExecutor + Send,
    C: Clock + Send + Sync,
{
    async fn run_all_scheduled_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        RedisBroker::run_all_scheduled_tasks(self, queue).await
    }

    async fn run_all_retry_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        RedisBroker::run_all_retry_tasks(self, queue).await
    }

    async fn run_all_archived_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        RedisBroker::run_all_archived_tasks(self, queue).await
    }

    async fn run_all_aggregating_tasks(
        &mut self,
        queue: &str,
        group: &str,
    ) -> Result<usize, AdminError> {
        RedisBroker::run_all_aggregating_tasks(self, queue, group).await
    }

    async fn archive_all_pending_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        RedisBroker::archive_all_pending_tasks(self, queue).await
    }

    async fn archive_all_scheduled_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        RedisBroker::archive_all_scheduled_tasks(self, queue).await
    }

    async fn archive_all_retry_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        RedisBroker::archive_all_retry_tasks(self, queue).await
    }

    async fn archive_all_aggregating_tasks(
        &mut self,
        queue: &str,
        group: &str,
    ) -> Result<usize, AdminError> {
        RedisBroker::archive_all_aggregating_tasks(self, queue, group).await
    }

    async fn delete_all_pending_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        RedisBroker::delete_all_pending_tasks(self, queue).await
    }

    async fn delete_all_scheduled_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        RedisBroker::delete_all_scheduled_tasks(self, queue).await
    }

    async fn delete_all_retry_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        RedisBroker::delete_all_retry_tasks(self, queue).await
    }

    async fn delete_all_archived_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        RedisBroker::delete_all_archived_tasks(self, queue).await
    }

    async fn delete_all_completed_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        RedisBroker::delete_all_completed_tasks(self, queue).await
    }

    async fn delete_all_aggregating_tasks(
        &mut self,
        queue: &str,
        group: &str,
    ) -> Result<usize, AdminError> {
        RedisBroker::delete_all_aggregating_tasks(self, queue, group).await
    }
}

#[async_trait::async_trait]
impl<E, C> InspectorTaskReadBroker for RedisBroker<E, C>
where
    E: RedisExecutor + Send,
    C: Clock + Send + Sync,
{
    async fn task_info(&mut self, queue: &str, task_id: &str) -> Result<TaskInfo, AdminError> {
        RedisBroker::task_info(self, queue, task_id).await
    }

    async fn list_pending_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        RedisBroker::list_pending_tasks(self, queue, pagination).await
    }

    async fn list_active_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        RedisBroker::list_active_tasks(self, queue, pagination).await
    }

    async fn list_aggregating_tasks(
        &mut self,
        queue: &str,
        group: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        RedisBroker::list_aggregating_tasks(self, queue, group, pagination).await
    }

    async fn list_scheduled_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        RedisBroker::list_scheduled_tasks(self, queue, pagination).await
    }

    async fn list_retry_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        RedisBroker::list_retry_tasks(self, queue, pagination).await
    }

    async fn list_archived_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        RedisBroker::list_archived_tasks(self, queue, pagination).await
    }

    async fn list_completed_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        RedisBroker::list_completed_tasks(self, queue, pagination).await
    }
}

#[async_trait::async_trait]
impl<E, C> InspectorMetadataBroker for RedisBroker<E, C>
where
    E: RedisExecutor + Send,
    C: Clock + Send + Sync,
{
    async fn servers(&mut self) -> Result<Vec<ServerInfo>, AdminError> {
        RedisBroker::servers(self).await
    }

    async fn cluster_key_slot(&mut self, queue: &str) -> Result<i64, AdminError> {
        RedisBroker::cluster_key_slot(self, queue).await
    }

    async fn cluster_nodes(&mut self, queue: &str) -> Result<Vec<ClusterNode>, AdminError> {
        RedisBroker::cluster_nodes(self, queue).await
    }

    async fn scheduler_entries(&mut self) -> Result<Vec<SchedulerEntryInfo>, AdminError> {
        RedisBroker::scheduler_entries(self).await
    }

    async fn list_scheduler_enqueue_events(
        &mut self,
        entry_id: &str,
        pagination: Pagination,
    ) -> Result<Vec<SchedulerEnqueueEventInfo>, AdminError> {
        RedisBroker::list_scheduler_enqueue_events(self, entry_id, pagination).await
    }
}
