pub(super) use super::*;
pub(super) use crate::admin::{
    CancelBroker, InspectorBulkBroker, InspectorMetadataBroker, InspectorQueueBroker,
    InspectorStatsBroker, InspectorTaskBroker, InspectorTaskReadBroker,
};
pub(super) use crate::client::CloseBroker;
pub(super) use crate::task::TaskMessage;
pub(super) use crate::{
    BrokerError, CancelError, EnqueueOptions, EnqueuePlan, MakeRedisClientError,
    RedisRuntimeClient, Task, TaskState, pb,
};
pub(super) use async_trait::async_trait;
pub(super) use prost::Message;
pub(super) use std::collections::HashMap;
pub(super) use std::time::{Duration, UNIX_EPOCH};

pub(super) fn inspector_with_broker<B>(broker: B) -> Inspector<B> {
    Inspector {
        broker,
        shared_connection: false,
    }
}

pub(super) fn shared_inspector_with_broker<B>(broker: B) -> Inspector<B> {
    Inspector {
        broker,
        shared_connection: true,
    }
}

#[derive(Debug, Default)]
struct RecordingCloseBroker {
    closed: bool,
    error: Option<BrokerError>,
}

impl CloseBroker for RecordingCloseBroker {
    fn close(&mut self) -> Result<(), BrokerError> {
        self.closed = true;
        if let Some(error) = self.error.clone() {
            return Err(error);
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct RecordingCancelBroker {
    published: Vec<String>,
    subscribers: usize,
    error: Option<CancelError>,
}

#[async_trait]
impl CancelBroker for RecordingCancelBroker {
    async fn publish_cancel(&mut self, task_id: &str) -> Result<usize, CancelError> {
        self.published.push(task_id.to_owned());
        if let Some(error) = self.error.clone() {
            return Err(error);
        }
        Ok(self.subscribers)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum QueueLifecycleCall {
    Pause(String),
    Unpause(String),
    Delete { queue: String, force: bool },
}

#[derive(Debug, Default)]
struct RecordingQueueLifecycleBroker {
    calls: Vec<QueueLifecycleCall>,
    error: Option<AdminError>,
}

#[async_trait]
impl InspectorQueueBroker for RecordingQueueLifecycleBroker {
    async fn pause_queue(&mut self, queue: &str) -> Result<(), AdminError> {
        self.calls.push(QueueLifecycleCall::Pause(queue.to_owned()));
        if let Some(error) = self.error.clone() {
            return Err(error);
        }
        Ok(())
    }

    async fn unpause_queue(&mut self, queue: &str) -> Result<(), AdminError> {
        self.calls
            .push(QueueLifecycleCall::Unpause(queue.to_owned()));
        if let Some(error) = self.error.clone() {
            return Err(error);
        }
        Ok(())
    }

    async fn delete_queue(&mut self, queue: &str, force: bool) -> Result<(), AdminError> {
        self.calls.push(QueueLifecycleCall::Delete {
            queue: queue.to_owned(),
            force,
        });
        if let Some(error) = self.error.clone() {
            return Err(error);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TaskLifecycleCall {
    Run {
        queue: String,
        task_id: String,
    },
    Archive {
        queue: String,
        task_id: String,
    },
    Delete {
        queue: String,
        task_id: String,
    },
    UpdatePayload {
        queue: String,
        task_id: String,
        payload: Vec<u8>,
    },
}

#[derive(Debug, Default)]
struct RecordingTaskLifecycleBroker {
    calls: Vec<TaskLifecycleCall>,
    error: Option<AdminError>,
}

#[async_trait]
impl InspectorTaskBroker for RecordingTaskLifecycleBroker {
    async fn run_task(&mut self, queue: &str, task_id: &str) -> Result<(), AdminError> {
        self.calls.push(TaskLifecycleCall::Run {
            queue: queue.to_owned(),
            task_id: task_id.to_owned(),
        });
        if let Some(error) = self.error.clone() {
            return Err(error);
        }
        Ok(())
    }

    async fn archive_task(&mut self, queue: &str, task_id: &str) -> Result<(), AdminError> {
        self.calls.push(TaskLifecycleCall::Archive {
            queue: queue.to_owned(),
            task_id: task_id.to_owned(),
        });
        if let Some(error) = self.error.clone() {
            return Err(error);
        }
        Ok(())
    }

    async fn delete_task(&mut self, queue: &str, task_id: &str) -> Result<(), AdminError> {
        self.calls.push(TaskLifecycleCall::Delete {
            queue: queue.to_owned(),
            task_id: task_id.to_owned(),
        });
        if let Some(error) = self.error.clone() {
            return Err(error);
        }
        Ok(())
    }

    async fn update_task_payload(
        &mut self,
        queue: &str,
        task_id: &str,
        payload: Vec<u8>,
    ) -> Result<(), AdminError> {
        self.calls.push(TaskLifecycleCall::UpdatePayload {
            queue: queue.to_owned(),
            task_id: task_id.to_owned(),
            payload,
        });
        if let Some(error) = self.error.clone() {
            return Err(error);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum StatsCall {
    Queues,
    Groups { queue: String },
    GetQueueInfo { queue: String },
    History { queue: String, days: isize },
}

#[derive(Debug, Clone)]
struct RecordingStatsBroker {
    calls: Vec<StatsCall>,
    queues: Vec<String>,
    groups: Vec<GroupStats>,
    queue_stats: QueueStats,
    history: Vec<DailyStats>,
    error: Option<AdminError>,
}

impl Default for RecordingStatsBroker {
    fn default() -> Self {
        Self {
            calls: Vec::new(),
            queues: Vec::new(),
            groups: Vec::new(),
            queue_stats: QueueStats::new(QueueStatsSnapshot {
                queue: "critical".to_owned(),
                memory_usage: 42,
                paused: false,
                states: QueueStateSnapshot {
                    groups: 2,
                    pending: 3,
                    active: 4,
                    scheduled: 5,
                    retry: 6,
                    archived: 7,
                    completed: 8,
                    aggregating: 9,
                },
                throughput: QueueThroughputSnapshot {
                    processed: 10,
                    failed: 11,
                    processed_total: 12,
                    failed_total: 13,
                },
                latency: Duration::from_secs(14),
                timestamp: UNIX_EPOCH,
            }),
            history: Vec::new(),
            error: None,
        }
    }
}

#[async_trait]
impl InspectorStatsBroker for RecordingStatsBroker {
    async fn list_queues(&mut self) -> Result<Vec<String>, AdminError> {
        self.calls.push(StatsCall::Queues);
        if let Some(error) = self.error.clone() {
            return Err(error);
        }
        Ok(self.queues.clone())
    }

    async fn group_stats(&mut self, queue: &str) -> Result<Vec<GroupStats>, AdminError> {
        self.calls.push(StatsCall::Groups {
            queue: queue.to_owned(),
        });
        if let Some(error) = self.error.clone() {
            return Err(error);
        }
        Ok(self.groups.clone())
    }

    async fn current_queue_stats(&mut self, queue: &str) -> Result<QueueStats, AdminError> {
        self.calls.push(StatsCall::GetQueueInfo {
            queue: queue.to_owned(),
        });
        if let Some(error) = self.error.clone() {
            return Err(error);
        }
        Ok(self.queue_stats.clone())
    }

    async fn historical_queue_stats(
        &mut self,
        queue: &str,
        days: isize,
    ) -> Result<Vec<DailyStats>, AdminError> {
        self.calls.push(StatsCall::History {
            queue: queue.to_owned(),
            days,
        });
        if let Some(error) = self.error.clone() {
            return Err(error);
        }
        Ok(self.history.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TaskReadCall {
    Get {
        queue: String,
        task_id: String,
    },
    Pending {
        queue: String,
        pagination: Pagination,
    },
    Active {
        queue: String,
        pagination: Pagination,
    },
    Aggregating {
        queue: String,
        group: String,
        pagination: Pagination,
    },
    Scheduled {
        queue: String,
        pagination: Pagination,
    },
    Retry {
        queue: String,
        pagination: Pagination,
    },
    Archived {
        queue: String,
        pagination: Pagination,
    },
    Completed {
        queue: String,
        pagination: Pagination,
    },
}

#[derive(Debug, Clone)]
struct RecordingTaskReadBroker {
    calls: Vec<TaskReadCall>,
    task: TaskInfo,
    tasks: Vec<TaskInfo>,
    error: Option<AdminError>,
}

impl Default for RecordingTaskReadBroker {
    fn default() -> Self {
        Self {
            calls: Vec::new(),
            task: sample_task_info("task-id", TaskState::Pending),
            tasks: Vec::new(),
            error: None,
        }
    }
}

#[async_trait]
impl InspectorTaskReadBroker for RecordingTaskReadBroker {
    async fn task_info(&mut self, queue: &str, task_id: &str) -> Result<TaskInfo, AdminError> {
        self.calls.push(TaskReadCall::Get {
            queue: queue.to_owned(),
            task_id: task_id.to_owned(),
        });
        if let Some(error) = self.error.clone() {
            return Err(error);
        }
        Ok(self.task.clone())
    }

    async fn list_pending_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        self.calls.push(TaskReadCall::Pending {
            queue: queue.to_owned(),
            pagination,
        });
        self.clone_tasks_or_error()
    }

    async fn list_active_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        self.calls.push(TaskReadCall::Active {
            queue: queue.to_owned(),
            pagination,
        });
        self.clone_tasks_or_error()
    }

    async fn list_aggregating_tasks(
        &mut self,
        queue: &str,
        group: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        self.calls.push(TaskReadCall::Aggregating {
            queue: queue.to_owned(),
            group: group.to_owned(),
            pagination,
        });
        self.clone_tasks_or_error()
    }

    async fn list_scheduled_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        self.calls.push(TaskReadCall::Scheduled {
            queue: queue.to_owned(),
            pagination,
        });
        self.clone_tasks_or_error()
    }

    async fn list_retry_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        self.calls.push(TaskReadCall::Retry {
            queue: queue.to_owned(),
            pagination,
        });
        self.clone_tasks_or_error()
    }

    async fn list_archived_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        self.calls.push(TaskReadCall::Archived {
            queue: queue.to_owned(),
            pagination,
        });
        self.clone_tasks_or_error()
    }

    async fn list_completed_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        self.calls.push(TaskReadCall::Completed {
            queue: queue.to_owned(),
            pagination,
        });
        self.clone_tasks_or_error()
    }
}

impl RecordingTaskReadBroker {
    fn clone_tasks_or_error(&self) -> Result<Vec<TaskInfo>, AdminError> {
        if let Some(error) = self.error.clone() {
            return Err(error);
        }
        Ok(self.tasks.clone())
    }
}

fn sample_task_info(id: &str, state: TaskState) -> TaskInfo {
    let message = TaskMessage {
        id: id.to_owned(),
        queue: "critical".to_owned(),
        r#type: "email:send".to_owned(),
        ..TaskMessage::default()
    };
    TaskInfo::new(message, state, false, None, Vec::new())
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BulkCall {
    RunScheduled(String),
    RunRetry(String),
    RunArchived(String),
    RunAggregating { queue: String, group: String },
    ArchivePending(String),
    ArchiveScheduled(String),
    ArchiveRetry(String),
    ArchiveAggregating { queue: String, group: String },
    DeletePending(String),
    DeleteScheduled(String),
    DeleteRetry(String),
    DeleteArchived(String),
    DeleteCompleted(String),
    DeleteAggregating { queue: String, group: String },
}

#[derive(Debug, Default)]
struct RecordingBulkBroker {
    calls: Vec<BulkCall>,
    affected: usize,
    error: Option<AdminError>,
}

#[async_trait]
impl InspectorBulkBroker for RecordingBulkBroker {
    async fn run_all_scheduled_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        self.record(BulkCall::RunScheduled(queue.to_owned()))
    }

    async fn run_all_retry_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        self.record(BulkCall::RunRetry(queue.to_owned()))
    }

    async fn run_all_archived_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        self.record(BulkCall::RunArchived(queue.to_owned()))
    }

    async fn run_all_aggregating_tasks(
        &mut self,
        queue: &str,
        group: &str,
    ) -> Result<usize, AdminError> {
        self.record(BulkCall::RunAggregating {
            queue: queue.to_owned(),
            group: group.to_owned(),
        })
    }

    async fn archive_all_pending_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        self.record(BulkCall::ArchivePending(queue.to_owned()))
    }

    async fn archive_all_scheduled_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        self.record(BulkCall::ArchiveScheduled(queue.to_owned()))
    }

    async fn archive_all_retry_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        self.record(BulkCall::ArchiveRetry(queue.to_owned()))
    }

    async fn archive_all_aggregating_tasks(
        &mut self,
        queue: &str,
        group: &str,
    ) -> Result<usize, AdminError> {
        self.record(BulkCall::ArchiveAggregating {
            queue: queue.to_owned(),
            group: group.to_owned(),
        })
    }

    async fn delete_all_pending_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        self.record(BulkCall::DeletePending(queue.to_owned()))
    }

    async fn delete_all_scheduled_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        self.record(BulkCall::DeleteScheduled(queue.to_owned()))
    }

    async fn delete_all_retry_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        self.record(BulkCall::DeleteRetry(queue.to_owned()))
    }

    async fn delete_all_archived_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        self.record(BulkCall::DeleteArchived(queue.to_owned()))
    }

    async fn delete_all_completed_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        self.record(BulkCall::DeleteCompleted(queue.to_owned()))
    }

    async fn delete_all_aggregating_tasks(
        &mut self,
        queue: &str,
        group: &str,
    ) -> Result<usize, AdminError> {
        self.record(BulkCall::DeleteAggregating {
            queue: queue.to_owned(),
            group: group.to_owned(),
        })
    }
}

impl RecordingBulkBroker {
    fn record(&mut self, call: BulkCall) -> Result<usize, AdminError> {
        self.calls.push(call);
        if let Some(error) = self.error.clone() {
            return Err(error);
        }
        Ok(self.affected)
    }
}

mod codec;
mod errors;
mod inspector;
mod inspector_metadata_support;
use inspector_metadata_support::*;
mod models;
mod pagination;
mod task_info;
