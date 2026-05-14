use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::keys;
use crate::message::{duration_seconds, unix_seconds};
use crate::{EnqueuePlan, TaskMessage, TaskState};

/// Default lease duration for a dequeued task.
///
/// Reference: Asynq v0.26.0 `LeaseDuration`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L46-L52>.
pub const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(30);

/// Expiration used for daily processed/failed counters.
///
/// Reference: Asynq v0.26.0 `statsTTL`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L54-L60>.
pub const STATS_TTL: Duration = Duration::from_secs(90 * 24 * 60 * 60);

/// Redis command intent for enqueueing a task.
///
/// Reference: Asynq v0.26.0 Redis enqueue scripts and RDB enqueue methods:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L6-L24>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisEnqueuePlan {
    operations: Vec<RedisEnqueueOperation>,
}

/// Redis command intent for dequeuing the next pending task.
///
/// Reference: Asynq v0.26.0 `RDB.Dequeue` scans queues and runs `dequeueCmd`
/// to move a task from pending to active with a lease:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L243-L274>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisDequeuePlan {
    queue_calls: Vec<RedisDequeueCall>,
    lease_expires_at: SystemTime,
}

/// Redis command intent for marking an active task as successfully completed.
///
/// Reference: Asynq v0.26.0 `RDB.Done` and `RDB.MarkAsComplete`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L325-L379>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisCompletePlan {
    call: RedisScriptCall,
}

/// Redis command intent for retrying a failed active task.
///
/// Reference: Asynq v0.26.0 `RDB.Retry`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L380-L418>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisRetryPlan {
    call: RedisScriptCall,
}

/// Redis command intent for archiving a failed active task.
///
/// Reference: Asynq v0.26.0 archive-related task lifecycle handling in
/// `internal/rdb`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisArchivePlan {
    call: RedisScriptCall,
}

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
    queue: String,
    keys: Vec<String>,
    args: Vec<RedisArg>,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedisArg {
    Bytes(Vec<u8>),
    String(String),
    I64(i64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedisEnqueuePlanError {
    UnsupportedState(TaskState),
    MissingScheduledProcessTime,
    MissingGroupKey,
    MissingUniqueKey,
    MissingUniqueLockTtl,
    TimeOverflow(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedisDequeuePlanError {
    EmptyQueueList,
    EmptyQueueName,
    TimeOverflow(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedisCompletePlanError {
    EmptyQueueName,
    EmptyTaskId,
    TimeOverflow(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedisRetryPlanError {
    EmptyQueueName,
    EmptyTaskId,
    TimeOverflow(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedisArchivePlanError {
    EmptyQueueName,
    EmptyTaskId,
    TimeOverflow(&'static str),
}

impl RedisEnqueuePlan {
    pub fn from_enqueue_plan(
        plan: &EnqueuePlan,
        now: SystemTime,
    ) -> Result<Self, RedisEnqueuePlanError> {
        let msg = plan.message();
        let queue = msg.queue.clone();
        let mut operations = vec![RedisEnqueueOperation::PublishQueue {
            key: keys::ALL_QUEUES.to_owned(),
            queue: queue.clone(),
        }];
        operations.push(RedisEnqueueOperation::EvalScript(script_call(plan, now)?));
        Ok(Self { operations })
    }

    pub fn operations(&self) -> &[RedisEnqueueOperation] {
        &self.operations
    }

    pub fn into_operations(self) -> Vec<RedisEnqueueOperation> {
        self.operations
    }
}

impl RedisDequeuePlan {
    pub fn from_queues(queues: &[String], now: SystemTime) -> Result<Self, RedisDequeuePlanError> {
        if queues.is_empty() {
            return Err(RedisDequeuePlanError::EmptyQueueList);
        }

        let lease_expires_at =
            now.checked_add(DEFAULT_LEASE_DURATION)
                .ok_or(RedisDequeuePlanError::TimeOverflow(
                    "dequeue lease expiration",
                ))?;
        let lease_expires_at_seconds = unix_seconds_checked(lease_expires_at, "dequeue lease")?;

        let mut queue_calls = Vec::with_capacity(queues.len());
        for queue in queues {
            if queue.trim().is_empty() {
                return Err(RedisDequeuePlanError::EmptyQueueName);
            }
            queue_calls.push(RedisDequeueCall {
                queue: queue.clone(),
                keys: vec![
                    keys::pending_key(queue),
                    keys::active_key(queue),
                    keys::lease_key(queue),
                    keys::task_key_prefix(queue),
                    keys::paused_key(queue),
                ],
                args: vec![RedisArg::I64(lease_expires_at_seconds)],
            });
        }

        Ok(Self {
            queue_calls,
            lease_expires_at,
        })
    }

    pub fn queue_calls(&self) -> &[RedisDequeueCall] {
        &self.queue_calls
    }

    pub fn lease_expires_at(&self) -> SystemTime {
        self.lease_expires_at
    }
}

impl RedisCompletePlan {
    pub fn from_message(
        message: &TaskMessage,
        now: SystemTime,
    ) -> Result<Self, RedisCompletePlanError> {
        if message.queue.trim().is_empty() {
            return Err(RedisCompletePlanError::EmptyQueueName);
        }
        if message.id.trim().is_empty() {
            return Err(RedisCompletePlanError::EmptyTaskId);
        }

        let call = if message.retention > 0 {
            mark_as_complete_call(message, now)?
        } else {
            done_call(message, now)?
        };
        Ok(Self { call })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}

impl RedisRetryPlan {
    pub fn from_message(
        message: &TaskMessage,
        now: SystemTime,
        retry_at: SystemTime,
        error_message: &str,
        is_failure: bool,
    ) -> Result<Self, RedisRetryPlanError> {
        if message.queue.trim().is_empty() {
            return Err(RedisRetryPlanError::EmptyQueueName);
        }
        if message.id.trim().is_empty() {
            return Err(RedisRetryPlanError::EmptyTaskId);
        }

        Ok(Self {
            call: retry_call(message, now, retry_at, error_message, is_failure)?,
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}

impl RedisArchivePlan {
    pub fn from_message(
        message: &TaskMessage,
        now: SystemTime,
        archived_at: SystemTime,
        error_message: &str,
        is_failure: bool,
    ) -> Result<Self, RedisArchivePlanError> {
        if message.queue.trim().is_empty() {
            return Err(RedisArchivePlanError::EmptyQueueName);
        }
        if message.id.trim().is_empty() {
            return Err(RedisArchivePlanError::EmptyTaskId);
        }

        Ok(Self {
            call: archive_call(message, now, archived_at, error_message, is_failure)?,
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
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

fn script_call(
    plan: &EnqueuePlan,
    now: SystemTime,
) -> Result<RedisScriptCall, RedisEnqueuePlanError> {
    let msg = plan.message();
    let unique = !msg.unique_key.is_empty();
    match (plan.state(), unique) {
        (TaskState::Pending, false) => pending_call(msg, now),
        (TaskState::Pending, true) => pending_unique_call(msg, plan, now),
        (TaskState::Scheduled, false) => scheduled_call(msg, plan),
        (TaskState::Scheduled, true) => scheduled_unique_call(msg, plan),
        (TaskState::Aggregating, false) => group_call(msg, now),
        (TaskState::Aggregating, true) => group_unique_call(msg, plan, now),
        (state, _) => Err(RedisEnqueuePlanError::UnsupportedState(state)),
    }
}

fn pending_call(
    msg: &TaskMessage,
    now: SystemTime,
) -> Result<RedisScriptCall, RedisEnqueuePlanError> {
    Ok(RedisScriptCall::new(
        RedisScript::Enqueue,
        vec![task_key(msg), keys::pending_key(&msg.queue)],
        vec![
            encoded_msg_arg(msg),
            RedisArg::String(msg.id.clone()),
            RedisArg::I64(unix_nanoseconds(now)?),
        ],
    ))
}

fn pending_unique_call(
    msg: &TaskMessage,
    plan: &EnqueuePlan,
    now: SystemTime,
) -> Result<RedisScriptCall, RedisEnqueuePlanError> {
    Ok(RedisScriptCall::new(
        RedisScript::EnqueueUnique,
        vec![
            unique_key(msg)?,
            task_key(msg),
            keys::pending_key(&msg.queue),
        ],
        vec![
            RedisArg::String(msg.id.clone()),
            RedisArg::I64(unique_ttl_seconds(plan)?),
            encoded_msg_arg(msg),
            RedisArg::I64(unix_nanoseconds(now)?),
        ],
    ))
}

fn scheduled_call(
    msg: &TaskMessage,
    plan: &EnqueuePlan,
) -> Result<RedisScriptCall, RedisEnqueuePlanError> {
    Ok(RedisScriptCall::new(
        RedisScript::Schedule,
        vec![task_key(msg), keys::scheduled_key(&msg.queue)],
        vec![
            encoded_msg_arg(msg),
            RedisArg::I64(scheduled_score(plan)?),
            RedisArg::String(msg.id.clone()),
        ],
    ))
}

fn scheduled_unique_call(
    msg: &TaskMessage,
    plan: &EnqueuePlan,
) -> Result<RedisScriptCall, RedisEnqueuePlanError> {
    Ok(RedisScriptCall::new(
        RedisScript::ScheduleUnique,
        vec![
            unique_key(msg)?,
            task_key(msg),
            keys::scheduled_key(&msg.queue),
        ],
        vec![
            RedisArg::String(msg.id.clone()),
            RedisArg::I64(unique_ttl_seconds(plan)?),
            RedisArg::I64(scheduled_score(plan)?),
            encoded_msg_arg(msg),
        ],
    ))
}

fn group_call(
    msg: &TaskMessage,
    now: SystemTime,
) -> Result<RedisScriptCall, RedisEnqueuePlanError> {
    let group = group_key(msg)?;
    Ok(RedisScriptCall::new(
        RedisScript::AddToGroup,
        vec![
            task_key(msg),
            keys::group_key(&msg.queue, group),
            keys::all_groups_key(&msg.queue),
        ],
        vec![
            encoded_msg_arg(msg),
            RedisArg::String(msg.id.clone()),
            RedisArg::I64(unix_seconds(now)),
            RedisArg::String(group.to_owned()),
        ],
    ))
}

fn group_unique_call(
    msg: &TaskMessage,
    plan: &EnqueuePlan,
    now: SystemTime,
) -> Result<RedisScriptCall, RedisEnqueuePlanError> {
    let group = group_key(msg)?;
    Ok(RedisScriptCall::new(
        RedisScript::AddToGroupUnique,
        vec![
            task_key(msg),
            keys::group_key(&msg.queue, group),
            keys::all_groups_key(&msg.queue),
            unique_key(msg)?,
        ],
        vec![
            encoded_msg_arg(msg),
            RedisArg::String(msg.id.clone()),
            RedisArg::I64(unix_seconds(now)),
            RedisArg::String(group.to_owned()),
            RedisArg::I64(unique_ttl_seconds(plan)?),
        ],
    ))
}

fn task_key(msg: &TaskMessage) -> String {
    keys::task_key(&msg.queue, &msg.id)
}

fn unique_key(msg: &TaskMessage) -> Result<String, RedisEnqueuePlanError> {
    if msg.unique_key.is_empty() {
        return Err(RedisEnqueuePlanError::MissingUniqueKey);
    }
    Ok(msg.unique_key.clone())
}

fn group_key(msg: &TaskMessage) -> Result<&str, RedisEnqueuePlanError> {
    if msg.group_key.is_empty() {
        return Err(RedisEnqueuePlanError::MissingGroupKey);
    }
    Ok(&msg.group_key)
}

fn unique_ttl_seconds(plan: &EnqueuePlan) -> Result<i64, RedisEnqueuePlanError> {
    plan.unique_lock_ttl()
        .map(duration_seconds)
        .ok_or(RedisEnqueuePlanError::MissingUniqueLockTtl)
}

fn scheduled_score(plan: &EnqueuePlan) -> Result<i64, RedisEnqueuePlanError> {
    plan.next_process_at()
        .map(unix_seconds)
        .ok_or(RedisEnqueuePlanError::MissingScheduledProcessTime)
}

fn encoded_msg_arg(msg: &TaskMessage) -> RedisArg {
    RedisArg::Bytes(msg.encode_to_vec())
}

fn done_call(
    msg: &TaskMessage,
    now: SystemTime,
) -> Result<RedisScriptCall, RedisCompletePlanError> {
    let script = if msg.unique_key.is_empty() {
        RedisScript::Done
    } else {
        RedisScript::DoneUnique
    };
    let mut keys = vec![
        keys::active_key(&msg.queue),
        keys::lease_key(&msg.queue),
        task_key(msg),
        keys::processed_key(&msg.queue, now),
        keys::processed_total_key(&msg.queue),
    ];
    if !msg.unique_key.is_empty() {
        keys.push(msg.unique_key.clone());
    }

    Ok(RedisScriptCall::new(
        script,
        keys,
        vec![
            RedisArg::String(msg.id.clone()),
            RedisArg::I64(stats_expire_at(now)?),
            RedisArg::I64(i64::MAX),
        ],
    ))
}

fn mark_as_complete_call(
    msg: &TaskMessage,
    now: SystemTime,
) -> Result<RedisScriptCall, RedisCompletePlanError> {
    let script = if msg.unique_key.is_empty() {
        RedisScript::MarkAsComplete
    } else {
        RedisScript::MarkAsCompleteUnique
    };
    let mut keys = vec![
        keys::active_key(&msg.queue),
        keys::lease_key(&msg.queue),
        keys::completed_key(&msg.queue),
        task_key(msg),
        keys::processed_key(&msg.queue, now),
        keys::processed_total_key(&msg.queue),
    ];
    if !msg.unique_key.is_empty() {
        keys.push(msg.unique_key.clone());
    }

    let completed_at = unix_seconds_complete(now, "complete time")?;
    let expires_at =
        completed_at
            .checked_add(msg.retention)
            .ok_or(RedisCompletePlanError::TimeOverflow(
                "completed task expiration",
            ))?;
    let mut completed = msg.clone();
    completed.completed_at = completed_at;

    Ok(RedisScriptCall::new(
        script,
        keys,
        vec![
            RedisArg::String(msg.id.clone()),
            RedisArg::I64(stats_expire_at(now)?),
            RedisArg::I64(expires_at),
            encoded_msg_arg(&completed),
            RedisArg::I64(i64::MAX),
        ],
    ))
}

fn retry_call(
    msg: &TaskMessage,
    now: SystemTime,
    retry_at: SystemTime,
    error_message: &str,
    is_failure: bool,
) -> Result<RedisScriptCall, RedisRetryPlanError> {
    let mut retry_message = msg.clone();
    retry_message.retried = retry_message.retried.saturating_add(1);
    retry_message.error_msg = error_message.to_owned();
    retry_message.last_failed_at = unix_seconds_retry(now, "last failed time")?;

    Ok(RedisScriptCall::new(
        RedisScript::Retry,
        vec![
            keys::active_key(&msg.queue),
            keys::lease_key(&msg.queue),
            keys::retry_key(&msg.queue),
            task_key(msg),
            keys::processed_key(&msg.queue, now),
            keys::processed_total_key(&msg.queue),
            keys::failed_key(&msg.queue, now),
            keys::failed_total_key(&msg.queue),
        ],
        vec![
            RedisArg::String(msg.id.clone()),
            encoded_msg_arg(&retry_message),
            RedisArg::I64(unix_seconds_retry(retry_at, "retry time")?),
            RedisArg::I64(stats_expire_at_retry(now)?),
            RedisArg::String(if is_failure { "1" } else { "0" }.to_owned()),
            RedisArg::I64(i64::MAX),
        ],
    ))
}

fn archive_call(
    msg: &TaskMessage,
    now: SystemTime,
    archived_at: SystemTime,
    error_message: &str,
    is_failure: bool,
) -> Result<RedisScriptCall, RedisArchivePlanError> {
    let mut archived_message = msg.clone();
    archived_message.retried = archived_message.retried.saturating_add(1);
    archived_message.error_msg = error_message.to_owned();
    archived_message.last_failed_at = unix_seconds_archive(now, "last failed time")?;

    Ok(RedisScriptCall::new(
        RedisScript::Archive,
        vec![
            keys::active_key(&msg.queue),
            keys::lease_key(&msg.queue),
            keys::archived_key(&msg.queue),
            task_key(msg),
            keys::processed_key(&msg.queue, now),
            keys::processed_total_key(&msg.queue),
            keys::failed_key(&msg.queue, now),
            keys::failed_total_key(&msg.queue),
        ],
        vec![
            RedisArg::String(msg.id.clone()),
            encoded_msg_arg(&archived_message),
            RedisArg::I64(unix_seconds_archive(archived_at, "archive time")?),
            RedisArg::I64(stats_expire_at_archive(now)?),
            RedisArg::String(if is_failure { "1" } else { "0" }.to_owned()),
            RedisArg::I64(i64::MAX),
        ],
    ))
}

fn stats_expire_at(now: SystemTime) -> Result<i64, RedisCompletePlanError> {
    let time = now
        .checked_add(STATS_TTL)
        .ok_or(RedisCompletePlanError::TimeOverflow("stats expiration"))?;
    unix_seconds_complete(time, "stats expiration")
}

fn stats_expire_at_retry(now: SystemTime) -> Result<i64, RedisRetryPlanError> {
    let time = now
        .checked_add(STATS_TTL)
        .ok_or(RedisRetryPlanError::TimeOverflow("stats expiration"))?;
    unix_seconds_retry(time, "stats expiration")
}

fn stats_expire_at_archive(now: SystemTime) -> Result<i64, RedisArchivePlanError> {
    let time = now
        .checked_add(STATS_TTL)
        .ok_or(RedisArchivePlanError::TimeOverflow("stats expiration"))?;
    unix_seconds_archive(time, "stats expiration")
}

fn unix_nanoseconds(time: SystemTime) -> Result<i64, RedisEnqueuePlanError> {
    let nanos = match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration_nanoseconds(duration),
        Err(error) => -duration_nanoseconds(error.duration()),
    };
    nanos
        .try_into()
        .map_err(|_| RedisEnqueuePlanError::TimeOverflow("unix nanoseconds"))
}

fn unix_seconds_checked(
    time: SystemTime,
    context: &'static str,
) -> Result<i64, RedisDequeuePlanError> {
    let seconds = match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => i128::from(duration_seconds(duration)),
        Err(error) => -i128::from(duration_seconds(error.duration())),
    };
    seconds
        .try_into()
        .map_err(|_| RedisDequeuePlanError::TimeOverflow(context))
}

fn unix_seconds_complete(
    time: SystemTime,
    context: &'static str,
) -> Result<i64, RedisCompletePlanError> {
    let seconds = match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => i128::from(duration_seconds(duration)),
        Err(error) => -i128::from(duration_seconds(error.duration())),
    };
    seconds
        .try_into()
        .map_err(|_| RedisCompletePlanError::TimeOverflow(context))
}

fn unix_seconds_retry(time: SystemTime, context: &'static str) -> Result<i64, RedisRetryPlanError> {
    let seconds = match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => i128::from(duration_seconds(duration)),
        Err(error) => -i128::from(duration_seconds(error.duration())),
    };
    seconds
        .try_into()
        .map_err(|_| RedisRetryPlanError::TimeOverflow(context))
}

fn unix_seconds_archive(
    time: SystemTime,
    context: &'static str,
) -> Result<i64, RedisArchivePlanError> {
    let seconds = match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => i128::from(duration_seconds(duration)),
        Err(error) => -i128::from(duration_seconds(error.duration())),
    };
    seconds
        .try_into()
        .map_err(|_| RedisArchivePlanError::TimeOverflow(context))
}

fn duration_nanoseconds(duration: Duration) -> i128 {
    i128::from(duration.as_secs()) * 1_000_000_000 + i128::from(duration.subsec_nanos())
}

impl std::fmt::Display for RedisEnqueuePlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedState(state) => {
                write!(f, "cannot enqueue Redis command plan for {state} state")
            }
            Self::MissingScheduledProcessTime => {
                f.write_str("scheduled enqueue plan is missing process time")
            }
            Self::MissingGroupKey => f.write_str("aggregating enqueue plan is missing group key"),
            Self::MissingUniqueKey => f.write_str("unique enqueue plan is missing unique key"),
            Self::MissingUniqueLockTtl => f.write_str("unique enqueue plan is missing lock ttl"),
            Self::TimeOverflow(context) => write!(f, "time overflow while computing {context}"),
        }
    }
}

impl std::error::Error for RedisEnqueuePlanError {}

impl std::fmt::Display for RedisDequeuePlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyQueueList => f.write_str("dequeue requires at least one queue"),
            Self::EmptyQueueName => f.write_str("queue name must contain one or more characters"),
            Self::TimeOverflow(context) => write!(f, "time overflow while computing {context}"),
        }
    }
}

impl std::error::Error for RedisDequeuePlanError {}

impl std::fmt::Display for RedisCompletePlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyQueueName => f.write_str("queue name must contain one or more characters"),
            Self::EmptyTaskId => f.write_str("task id must contain one or more characters"),
            Self::TimeOverflow(context) => write!(f, "time overflow while computing {context}"),
        }
    }
}

impl std::error::Error for RedisCompletePlanError {}

impl std::fmt::Display for RedisRetryPlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyQueueName => f.write_str("queue name must contain one or more characters"),
            Self::EmptyTaskId => f.write_str("task id must contain one or more characters"),
            Self::TimeOverflow(context) => write!(f, "time overflow while computing {context}"),
        }
    }
}

impl std::error::Error for RedisRetryPlanError {}

impl std::fmt::Display for RedisArchivePlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyQueueName => f.write_str("queue name must contain one or more characters"),
            Self::EmptyTaskId => f.write_str("task id must contain one or more characters"),
            Self::TimeOverflow(context) => write!(f, "time overflow while computing {context}"),
        }
    }
}

impl std::error::Error for RedisArchivePlanError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Task, TaskOption};

    #[test]
    fn plans_pending_enqueue_script() {
        let plan_now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let exec_now = plan_now + Duration::from_secs(10);
        let task = Task::new_with_options(
            "email:welcome",
            b"payload".to_vec(),
            [TaskOption::queue("critical")],
        );
        let enqueue_plan = EnqueuePlan::from_task(&task, plan_now, "task-id").unwrap();

        let redis_plan = RedisEnqueuePlan::from_enqueue_plan(&enqueue_plan, exec_now).unwrap();

        assert_eq!(
            redis_plan.operations()[0],
            RedisEnqueueOperation::PublishQueue {
                key: keys::ALL_QUEUES.to_owned(),
                queue: "critical".to_owned()
            }
        );
        let call = only_script(&redis_plan);
        assert_eq!(call.script(), RedisScript::Enqueue);
        assert_eq!(
            call.keys(),
            &[
                "asynq:{critical}:t:task-id".to_owned(),
                "asynq:{critical}:pending".to_owned(),
            ]
        );
        assert_encoded_message(&call.args()[0], enqueue_plan.message());
        assert_eq!(call.args()[1], RedisArg::String("task-id".to_owned()));
        assert_eq!(call.args()[2], RedisArg::I64(1_700_000_010_000_000_000));
    }

    #[test]
    fn plans_pending_unique_enqueue_script() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let task = Task::new_with_options(
            "email:welcome",
            b"payload".to_vec(),
            [
                TaskOption::queue("critical"),
                TaskOption::unique(Duration::from_secs(300)),
            ],
        );
        let enqueue_plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();

        let redis_plan = RedisEnqueuePlan::from_enqueue_plan(&enqueue_plan, now).unwrap();

        let call = only_script(&redis_plan);
        assert_eq!(call.script(), RedisScript::EnqueueUnique);
        assert_eq!(
            call.keys(),
            &[
                "asynq:{critical}:unique:email:welcome:321c3cf486ed509164edec1e1981fec8".to_owned(),
                "asynq:{critical}:t:task-id".to_owned(),
                "asynq:{critical}:pending".to_owned(),
            ]
        );
        assert_eq!(call.args()[0], RedisArg::String("task-id".to_owned()));
        assert_eq!(call.args()[1], RedisArg::I64(300));
        assert_encoded_message(&call.args()[2], enqueue_plan.message());
        assert_eq!(call.args()[3], RedisArg::I64(1_700_000_000_000_000_000));
    }

    #[test]
    fn plans_scheduled_enqueue_script() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let process_at = now + Duration::from_secs(60);
        let task = Task::new_with_options(
            "email:welcome",
            Vec::new(),
            [
                TaskOption::queue("critical"),
                TaskOption::process_at(process_at),
            ],
        );
        let enqueue_plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();

        let redis_plan = RedisEnqueuePlan::from_enqueue_plan(&enqueue_plan, now).unwrap();

        let call = only_script(&redis_plan);
        assert_eq!(call.script(), RedisScript::Schedule);
        assert_eq!(
            call.keys(),
            &[
                "asynq:{critical}:t:task-id".to_owned(),
                "asynq:{critical}:scheduled".to_owned(),
            ]
        );
        assert_encoded_message(&call.args()[0], enqueue_plan.message());
        assert_eq!(call.args()[1], RedisArg::I64(1_700_000_060));
        assert_eq!(call.args()[2], RedisArg::String("task-id".to_owned()));
    }

    #[test]
    fn plans_scheduled_unique_enqueue_script_with_extended_unique_ttl() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let task = Task::new_with_options(
            "email:welcome",
            b"payload".to_vec(),
            [
                TaskOption::queue("critical"),
                TaskOption::process_in(Duration::from_secs(60)),
                TaskOption::unique(Duration::from_secs(300)),
            ],
        );
        let enqueue_plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();

        let redis_plan = RedisEnqueuePlan::from_enqueue_plan(&enqueue_plan, now).unwrap();

        let call = only_script(&redis_plan);
        assert_eq!(call.script(), RedisScript::ScheduleUnique);
        assert_eq!(
            call.keys(),
            &[
                "asynq:{critical}:unique:email:welcome:321c3cf486ed509164edec1e1981fec8".to_owned(),
                "asynq:{critical}:t:task-id".to_owned(),
                "asynq:{critical}:scheduled".to_owned(),
            ]
        );
        assert_eq!(call.args()[0], RedisArg::String("task-id".to_owned()));
        assert_eq!(call.args()[1], RedisArg::I64(360));
        assert_eq!(call.args()[2], RedisArg::I64(1_700_000_060));
        assert_encoded_message(&call.args()[3], enqueue_plan.message());
    }

    #[test]
    fn plans_group_enqueue_script() {
        let plan_now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let exec_now = plan_now + Duration::from_secs(10);
        let task = Task::new_with_options(
            "email:welcome",
            Vec::new(),
            [TaskOption::queue("critical"), TaskOption::group("tenant-a")],
        );
        let enqueue_plan = EnqueuePlan::from_task(&task, plan_now, "task-id").unwrap();

        let redis_plan = RedisEnqueuePlan::from_enqueue_plan(&enqueue_plan, exec_now).unwrap();

        let call = only_script(&redis_plan);
        assert_eq!(call.script(), RedisScript::AddToGroup);
        assert_eq!(
            call.keys(),
            &[
                "asynq:{critical}:t:task-id".to_owned(),
                "asynq:{critical}:g:tenant-a".to_owned(),
                "asynq:{critical}:groups".to_owned(),
            ]
        );
        assert_encoded_message(&call.args()[0], enqueue_plan.message());
        assert_eq!(call.args()[1], RedisArg::String("task-id".to_owned()));
        assert_eq!(call.args()[2], RedisArg::I64(1_700_000_010));
        assert_eq!(call.args()[3], RedisArg::String("tenant-a".to_owned()));
    }

    #[test]
    fn plans_group_unique_enqueue_script() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let task = Task::new_with_options(
            "email:welcome",
            b"payload".to_vec(),
            [
                TaskOption::queue("critical"),
                TaskOption::group("tenant-a"),
                TaskOption::unique(Duration::from_secs(300)),
            ],
        );
        let enqueue_plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();

        let redis_plan = RedisEnqueuePlan::from_enqueue_plan(&enqueue_plan, now).unwrap();

        let call = only_script(&redis_plan);
        assert_eq!(call.script(), RedisScript::AddToGroupUnique);
        assert_eq!(
            call.keys(),
            &[
                "asynq:{critical}:t:task-id".to_owned(),
                "asynq:{critical}:g:tenant-a".to_owned(),
                "asynq:{critical}:groups".to_owned(),
                "asynq:{critical}:unique:email:welcome:321c3cf486ed509164edec1e1981fec8".to_owned(),
            ]
        );
        assert_encoded_message(&call.args()[0], enqueue_plan.message());
        assert_eq!(call.args()[1], RedisArg::String("task-id".to_owned()));
        assert_eq!(call.args()[2], RedisArg::I64(1_700_000_000));
        assert_eq!(call.args()[3], RedisArg::String("tenant-a".to_owned()));
        assert_eq!(call.args()[4], RedisArg::I64(300));
    }

    #[test]
    fn plans_dequeue_calls_for_queues() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let queues = vec!["critical".to_owned(), "default".to_owned()];

        let plan = RedisDequeuePlan::from_queues(&queues, now).unwrap();

        assert_eq!(
            plan.lease_expires_at(),
            UNIX_EPOCH + Duration::from_secs(1_700_000_030)
        );
        assert_eq!(plan.queue_calls().len(), 2);
        let call = &plan.queue_calls()[0];
        assert_eq!(call.queue(), "critical");
        assert_eq!(call.script(), RedisScript::Dequeue);
        assert_eq!(
            call.keys(),
            &[
                "asynq:{critical}:pending".to_owned(),
                "asynq:{critical}:active".to_owned(),
                "asynq:{critical}:lease".to_owned(),
                "asynq:{critical}:t:".to_owned(),
                "asynq:{critical}:paused".to_owned(),
            ]
        );
        assert_eq!(call.args(), &[RedisArg::I64(1_700_000_030)]);
    }

    #[test]
    fn validates_dequeue_inputs() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

        assert_eq!(
            RedisDequeuePlan::from_queues(&[], now).unwrap_err(),
            RedisDequeuePlanError::EmptyQueueList
        );
        assert_eq!(
            RedisDequeuePlan::from_queues(&[" ".to_owned()], now).unwrap_err(),
            RedisDequeuePlanError::EmptyQueueName
        );
    }

    #[test]
    fn plans_done_script_for_zero_retention_task() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let msg = active_message(0, "");

        let plan = RedisCompletePlan::from_message(&msg, now).unwrap();
        let call = plan.call();

        assert_eq!(call.script(), RedisScript::Done);
        assert_eq!(
            call.keys(),
            &[
                "asynq:{critical}:active".to_owned(),
                "asynq:{critical}:lease".to_owned(),
                "asynq:{critical}:t:task-id".to_owned(),
                "asynq:{critical}:processed:2023-11-14".to_owned(),
                "asynq:{critical}:processed".to_owned(),
            ]
        );
        assert_eq!(call.args()[0], RedisArg::String("task-id".to_owned()));
        assert_eq!(call.args()[1], RedisArg::I64(1_707_776_000));
        assert_eq!(call.args()[2], RedisArg::I64(i64::MAX));
    }

    #[test]
    fn plans_done_unique_script_for_zero_retention_unique_task() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let msg = active_message(
            0,
            "asynq:{critical}:unique:email:welcome:321c3cf486ed509164edec1e1981fec8",
        );

        let plan = RedisCompletePlan::from_message(&msg, now).unwrap();
        let call = plan.call();

        assert_eq!(call.script(), RedisScript::DoneUnique);
        assert_eq!(
            call.keys()[5],
            "asynq:{critical}:unique:email:welcome:321c3cf486ed509164edec1e1981fec8"
        );
        assert_eq!(call.args()[0], RedisArg::String("task-id".to_owned()));
    }

    #[test]
    fn plans_mark_as_complete_script_for_retained_task() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let msg = active_message(300, "");

        let plan = RedisCompletePlan::from_message(&msg, now).unwrap();
        let call = plan.call();

        assert_eq!(call.script(), RedisScript::MarkAsComplete);
        assert_eq!(
            call.keys(),
            &[
                "asynq:{critical}:active".to_owned(),
                "asynq:{critical}:lease".to_owned(),
                "asynq:{critical}:completed".to_owned(),
                "asynq:{critical}:t:task-id".to_owned(),
                "asynq:{critical}:processed:2023-11-14".to_owned(),
                "asynq:{critical}:processed".to_owned(),
            ]
        );
        assert_eq!(call.args()[0], RedisArg::String("task-id".to_owned()));
        assert_eq!(call.args()[1], RedisArg::I64(1_707_776_000));
        assert_eq!(call.args()[2], RedisArg::I64(1_700_000_300));
        assert_completed_message(&call.args()[3], &msg, 1_700_000_000);
        assert_eq!(call.args()[4], RedisArg::I64(i64::MAX));
    }

    #[test]
    fn plans_mark_as_complete_unique_script_for_retained_unique_task() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let msg = active_message(
            300,
            "asynq:{critical}:unique:email:welcome:321c3cf486ed509164edec1e1981fec8",
        );

        let plan = RedisCompletePlan::from_message(&msg, now).unwrap();
        let call = plan.call();

        assert_eq!(call.script(), RedisScript::MarkAsCompleteUnique);
        assert_eq!(
            call.keys()[6],
            "asynq:{critical}:unique:email:welcome:321c3cf486ed509164edec1e1981fec8"
        );
        assert_completed_message(&call.args()[3], &msg, 1_700_000_000);
    }

    #[test]
    fn validates_complete_inputs() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let mut msg = active_message(0, "");

        msg.queue = " ".to_owned();
        assert_eq!(
            RedisCompletePlan::from_message(&msg, now).unwrap_err(),
            RedisCompletePlanError::EmptyQueueName
        );

        msg.queue = "critical".to_owned();
        msg.id = " ".to_owned();
        assert_eq!(
            RedisCompletePlan::from_message(&msg, now).unwrap_err(),
            RedisCompletePlanError::EmptyTaskId
        );
    }

    #[test]
    fn plans_retry_script_for_failed_active_task() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let retry_at = now + Duration::from_secs(60);
        let msg = active_message(0, "");

        let plan =
            RedisRetryPlan::from_message(&msg, now, retry_at, "handler failed", true).unwrap();
        let call = plan.call();

        assert_eq!(call.script(), RedisScript::Retry);
        assert_eq!(
            call.keys(),
            &[
                "asynq:{critical}:active".to_owned(),
                "asynq:{critical}:lease".to_owned(),
                "asynq:{critical}:retry".to_owned(),
                "asynq:{critical}:t:task-id".to_owned(),
                "asynq:{critical}:processed:2023-11-14".to_owned(),
                "asynq:{critical}:processed".to_owned(),
                "asynq:{critical}:failed:2023-11-14".to_owned(),
                "asynq:{critical}:failed".to_owned(),
            ]
        );
        assert_eq!(call.args()[0], RedisArg::String("task-id".to_owned()));
        assert_retry_message(&call.args()[1], &msg, "handler failed", 1_700_000_000);
        assert_eq!(call.args()[2], RedisArg::I64(1_700_000_060));
        assert_eq!(call.args()[3], RedisArg::I64(1_707_776_000));
        assert_eq!(call.args()[4], RedisArg::String("1".to_owned()));
        assert_eq!(call.args()[5], RedisArg::I64(i64::MAX));
    }

    #[test]
    fn validates_retry_inputs() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let mut msg = active_message(0, "");

        msg.queue = " ".to_owned();
        assert_eq!(
            RedisRetryPlan::from_message(&msg, now, now, "handler failed", true).unwrap_err(),
            RedisRetryPlanError::EmptyQueueName
        );

        msg.queue = "critical".to_owned();
        msg.id = " ".to_owned();
        assert_eq!(
            RedisRetryPlan::from_message(&msg, now, now, "handler failed", true).unwrap_err(),
            RedisRetryPlanError::EmptyTaskId
        );
    }

    #[test]
    fn plans_archive_script_for_failed_active_task() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let msg = active_message(0, "");

        let plan =
            RedisArchivePlan::from_message(&msg, now, now, "max retry exhausted", true).unwrap();
        let call = plan.call();

        assert_eq!(call.script(), RedisScript::Archive);
        assert_eq!(
            call.keys(),
            &[
                "asynq:{critical}:active".to_owned(),
                "asynq:{critical}:lease".to_owned(),
                "asynq:{critical}:archived".to_owned(),
                "asynq:{critical}:t:task-id".to_owned(),
                "asynq:{critical}:processed:2023-11-14".to_owned(),
                "asynq:{critical}:processed".to_owned(),
                "asynq:{critical}:failed:2023-11-14".to_owned(),
                "asynq:{critical}:failed".to_owned(),
            ]
        );
        assert_eq!(call.args()[0], RedisArg::String("task-id".to_owned()));
        assert_failure_message(&call.args()[1], &msg, "max retry exhausted", 1_700_000_000);
        assert_eq!(call.args()[2], RedisArg::I64(1_700_000_000));
        assert_eq!(call.args()[3], RedisArg::I64(1_707_776_000));
        assert_eq!(call.args()[4], RedisArg::String("1".to_owned()));
        assert_eq!(call.args()[5], RedisArg::I64(i64::MAX));
    }

    #[test]
    fn validates_archive_inputs() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let mut msg = active_message(0, "");

        msg.queue = " ".to_owned();
        assert_eq!(
            RedisArchivePlan::from_message(&msg, now, now, "max retry exhausted", true)
                .unwrap_err(),
            RedisArchivePlanError::EmptyQueueName
        );

        msg.queue = "critical".to_owned();
        msg.id = " ".to_owned();
        assert_eq!(
            RedisArchivePlan::from_message(&msg, now, now, "max retry exhausted", true)
                .unwrap_err(),
            RedisArchivePlanError::EmptyTaskId
        );
    }

    fn only_script(plan: &RedisEnqueuePlan) -> &RedisScriptCall {
        assert_eq!(plan.operations().len(), 2);
        match &plan.operations()[1] {
            RedisEnqueueOperation::EvalScript(call) => call,
            operation => panic!("expected script operation, got {operation:?}"),
        }
    }

    fn assert_encoded_message(arg: &RedisArg, expected: &TaskMessage) {
        let RedisArg::Bytes(data) = arg else {
            panic!("expected encoded message bytes, got {arg:?}");
        };
        let decoded = TaskMessage::decode_from_slice(data).unwrap();
        assert_eq!(&decoded, expected);
    }

    fn assert_completed_message(arg: &RedisArg, original: &TaskMessage, completed_at: i64) {
        let RedisArg::Bytes(data) = arg else {
            panic!("expected encoded message bytes, got {arg:?}");
        };
        let decoded = TaskMessage::decode_from_slice(data).unwrap();
        assert_eq!(decoded.completed_at, completed_at);
        let mut expected = original.clone();
        expected.completed_at = completed_at;
        assert_eq!(decoded, expected);
    }

    fn assert_retry_message(
        arg: &RedisArg,
        original: &TaskMessage,
        error_message: &str,
        last_failed_at: i64,
    ) {
        assert_failure_message(arg, original, error_message, last_failed_at);
    }

    fn assert_failure_message(
        arg: &RedisArg,
        original: &TaskMessage,
        error_message: &str,
        last_failed_at: i64,
    ) {
        let RedisArg::Bytes(data) = arg else {
            panic!("expected encoded message bytes, got {arg:?}");
        };
        let decoded = TaskMessage::decode_from_slice(data).unwrap();
        let mut expected = original.clone();
        expected.retried += 1;
        expected.error_msg = error_message.to_owned();
        expected.last_failed_at = last_failed_at;
        assert_eq!(decoded, expected);
    }

    fn active_message(retention: i64, unique_key: &str) -> TaskMessage {
        let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
        msg.id = "task-id".to_owned();
        msg.queue = "critical".to_owned();
        msg.retention = retention;
        msg.unique_key = unique_key.to_owned();
        msg
    }
}
