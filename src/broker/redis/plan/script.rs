/// Redis command operation used while enqueueing a task.
///
/// Reference: Asynq v0.26.0 enqueue publishes the queue name and executes the
/// enqueue Lua script as separate Redis operations:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L101-L194>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedisEnqueueOperation {
    PublishQueue { key: String, queue: String },
    EvalScript(RedisScriptCall),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisScriptCall {
    script: RedisScript,
    keys: Vec<String>,
    args: Vec<RedisArg>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisDequeueCall {
    pub(crate) queue: String,
    pub(crate) keys: Vec<String>,
    pub(crate) args: Vec<RedisArg>,
}

/// Fixed Redis Lua scripts used by Asynq task lifecycle operations.
///
/// Reference: Asynq v0.26.0 RDB scripts and methods:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L735>.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisScript {
    Enqueue,
    EnqueueUnique,
    Schedule,
    ScheduleUnique,
    AddToGroup,
    AddToGroupUnique,
    Dequeue,
    Done,
    DoneUnique,
    MarkAsComplete,
    MarkAsCompleteUnique,
    Retry,
    Archive,
    Requeue,
    Forward,
    DeleteExpiredCompletedTasks,
    AggregationCheck,
    ReadAggregationSet,
    DeleteAggregationSet,
    ReclaimStaleAggregationSets,
    WriteServerState,
    ClearServerState,
    ListServerKeys,
    ListWorkerKeys,
    WriteSchedulerEntries,
    ListSchedulerEntries,
    RecordSchedulerEnqueueEvent,
    DeleteQueue,
    DeleteQueueForce,
    DeleteTask,
    RunTask,
    ArchiveTask,
    UpdateTaskPayload,
    CurrentQueueStats,
    QueueMemoryUsage,
    HistoricalQueueStats,
    GroupStats,
    TaskInfo,
    ListTasks,
    RunAllTasks,
    ArchiveAllTasks,
    ArchiveAllPendingTasks,
    DeleteAllTasks,
    DeleteAllPendingTasks,
    RunAllAggregatingTasks,
    ArchiveAllAggregatingTasks,
    DeleteAllAggregatingTasks,
    ListLeaseExpired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedisArg {
    Bytes(Vec<u8>),
    String(String),
    I64(i64),
}

impl RedisScriptCall {
    pub fn new(script: RedisScript, keys: Vec<String>, args: Vec<RedisArg>) -> Self {
        Self { script, keys, args }
    }

    pub fn script(&self) -> RedisScript {
        self.script
    }

    pub fn keys(&self) -> &[String] {
        &self.keys
    }

    pub fn args(&self) -> &[RedisArg] {
        &self.args
    }
}

impl RedisDequeueCall {
    pub fn queue(&self) -> &str {
        &self.queue
    }

    pub fn script(&self) -> RedisScript {
        RedisScript::Dequeue
    }

    pub fn keys(&self) -> &[String] {
        &self.keys
    }

    pub fn args(&self) -> &[RedisArg] {
        &self.args
    }
}
