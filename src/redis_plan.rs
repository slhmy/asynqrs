use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::keys;
use crate::message::{duration_seconds, unix_seconds};
use crate::{EnqueuePlan, TaskMessage, TaskState};

/// Default lease duration for a dequeued task.
///
/// Reference: Asynq v0.26.0 `LeaseDuration`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L46-L52>.
pub const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(30);

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
}
