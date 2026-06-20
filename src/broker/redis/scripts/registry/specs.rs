use crate::broker::redis::RedisScript;

use super::super::RedisScriptSpec;

impl RedisScript {
    /// Reference: Asynq v0.26.0 Redis scripts are grouped here by the
    /// lifecycle or inspector operation that owns each command:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L735>.
    pub const fn spec(self) -> RedisScriptSpec {
        match self {
            Self::Enqueue
            | Self::EnqueueUnique
            | Self::Schedule
            | Self::ScheduleUnique
            | Self::AddToGroup
            | Self::AddToGroupUnique => super::enqueue::spec(self),
            Self::Dequeue
            | Self::Done
            | Self::DoneUnique
            | Self::MarkAsComplete
            | Self::MarkAsCompleteUnique
            | Self::Retry
            | Self::Archive
            | Self::Requeue
            | Self::Forward
            | Self::DeleteExpiredCompletedTasks
            | Self::ListLeaseExpired => super::lifecycle::spec(self),
            Self::AggregationCheck
            | Self::ReadAggregationSet
            | Self::DeleteAggregationSet
            | Self::ReclaimStaleAggregationSets => super::aggregation::spec(self),
            Self::WriteServerState
            | Self::ClearServerState
            | Self::ListServerKeys
            | Self::ListWorkerKeys
            | Self::WriteSchedulerEntries
            | Self::ListSchedulerEntries
            | Self::RecordSchedulerEnqueueEvent => super::metadata::spec(self),
            Self::DeleteQueue
            | Self::DeleteQueueForce
            | Self::DeleteTask
            | Self::RunTask
            | Self::ArchiveTask
            | Self::UpdateTaskPayload
            | Self::CurrentQueueStats
            | Self::QueueMemoryUsage
            | Self::HistoricalQueueStats
            | Self::GroupStats
            | Self::TaskInfo
            | Self::ListTasks
            | Self::RunAllTasks
            | Self::ArchiveAllTasks
            | Self::ArchiveAllPendingTasks
            | Self::DeleteAllTasks
            | Self::DeleteAllPendingTasks
            | Self::RunAllAggregatingTasks
            | Self::ArchiveAllAggregatingTasks
            | Self::DeleteAllAggregatingTasks => super::admin::spec(self),
        }
    }
}
