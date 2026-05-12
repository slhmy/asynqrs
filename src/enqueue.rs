use std::time::{Duration, SystemTime};

use crate::keys;
use crate::message::{duration_seconds, unix_seconds};
use crate::{Task, TaskMessage, TaskOption, TaskState};

/// Default max retry count used if nothing is specified.
///
/// Reference: Asynq v0.26.0 `defaultMaxRetry`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L229-L233>.
pub const DEFAULT_MAX_RETRY: i32 = 25;

/// Default timeout used if both timeout and deadline are not specified.
///
/// Reference: Asynq v0.26.0 `defaultTimeout`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L229-L233>.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// Pure enqueue decision model.
///
/// Reference: Asynq v0.26.0 `Client.EnqueueContext` composes options, builds
/// `base.TaskMessage`, and selects pending, scheduled, or aggregating state:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L271-L331>.
#[derive(Debug, Clone, PartialEq)]
pub struct EnqueuePlan {
    message: TaskMessage,
    state: TaskState,
    next_process_at: Option<SystemTime>,
    unique_lock_ttl: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnqueuePlanError {
    EmptyTaskType,
    EmptyQueueName,
    EmptyTaskId,
    UniqueTtlTooShort,
    EmptyGroupKey,
    TimeOverflow(&'static str),
}

impl EnqueuePlan {
    pub fn from_task(
        task: &Task,
        now: SystemTime,
        generated_task_id: impl Into<String>,
    ) -> Result<Self, EnqueuePlanError> {
        Self::from_task_with_options(task, [], now, generated_task_id)
    }

    pub fn from_task_with_options<I>(
        task: &Task,
        options: I,
        now: SystemTime,
        generated_task_id: impl Into<String>,
    ) -> Result<Self, EnqueuePlanError>
    where
        I: IntoIterator<Item = TaskOption>,
    {
        if is_blank(task.type_name()) {
            return Err(EnqueuePlanError::EmptyTaskType);
        }

        let mut opts = ComposedOptions::new(now, generated_task_id.into())?;
        for opt in task.options() {
            opts.apply(opt, now)?;
        }
        for opt in options {
            opts.apply(&opt, now)?;
        }
        opts.validate()?;

        let deadline = opts.deadline.map(unix_seconds).unwrap_or(0);
        let mut timeout = duration_seconds(opts.timeout);
        if opts.deadline.is_none() && opts.timeout == Duration::ZERO {
            timeout = duration_seconds(DEFAULT_TIMEOUT);
        }

        let unique_key = opts
            .unique_ttl
            .map(|_| keys::unique_key(&opts.queue, task.type_name(), task.payload()))
            .unwrap_or_default();

        let message = TaskMessage {
            r#type: task.type_name().to_owned(),
            payload: task.payload().to_vec(),
            headers: task.headers().clone(),
            id: opts.task_id,
            queue: opts.queue,
            retry: opts.retry,
            retried: 0,
            error_msg: String::new(),
            last_failed_at: 0,
            timeout,
            deadline,
            unique_key,
            group_key: opts.group.unwrap_or_default(),
            retention: duration_seconds(opts.retention),
            completed_at: 0,
        };

        let process_delay = opts.process_at.duration_since(now).ok();
        let state = if process_delay.is_some_and(|duration| duration > Duration::ZERO) {
            TaskState::Scheduled
        } else if !message.group_key.is_empty() {
            TaskState::Aggregating
        } else {
            TaskState::Pending
        };

        let next_process_at = match state {
            TaskState::Scheduled => Some(opts.process_at),
            TaskState::Pending => Some(now),
            TaskState::Aggregating => None,
            _ => unreachable!("enqueue only creates pending, scheduled, or aggregating tasks"),
        };

        let unique_lock_ttl = match (opts.unique_ttl, state) {
            (Some(ttl), TaskState::Scheduled) => Some(
                process_delay
                    .and_then(|delay| delay.checked_add(ttl))
                    .ok_or(EnqueuePlanError::TimeOverflow("scheduled unique ttl"))?,
            ),
            (Some(ttl), _) => Some(ttl),
            (None, _) => None,
        };

        Ok(Self {
            message,
            state,
            next_process_at,
            unique_lock_ttl,
        })
    }

    pub fn message(&self) -> &TaskMessage {
        &self.message
    }

    pub fn into_message(self) -> TaskMessage {
        self.message
    }

    pub fn state(&self) -> TaskState {
        self.state
    }

    pub fn next_process_at(&self) -> Option<SystemTime> {
        self.next_process_at
    }

    pub fn unique_lock_ttl(&self) -> Option<Duration> {
        self.unique_lock_ttl
    }
}

#[derive(Debug)]
struct ComposedOptions {
    retry: i32,
    queue: String,
    task_id: String,
    timeout: Duration,
    deadline: Option<SystemTime>,
    unique_ttl: Option<Duration>,
    process_at: SystemTime,
    retention: Duration,
    group: Option<String>,
}

impl ComposedOptions {
    fn new(now: SystemTime, generated_task_id: String) -> Result<Self, EnqueuePlanError> {
        if is_blank(&generated_task_id) {
            return Err(EnqueuePlanError::EmptyTaskId);
        }

        Ok(Self {
            retry: DEFAULT_MAX_RETRY,
            queue: keys::DEFAULT_QUEUE_NAME.to_owned(),
            task_id: generated_task_id,
            timeout: Duration::ZERO,
            deadline: None,
            unique_ttl: None,
            process_at: now,
            retention: Duration::ZERO,
            group: None,
        })
    }

    fn apply(&mut self, opt: &TaskOption, now: SystemTime) -> Result<(), EnqueuePlanError> {
        match opt {
            TaskOption::MaxRetry(retry) => {
                self.retry = (*retry).try_into().unwrap_or(i32::MAX);
            }
            TaskOption::Queue(queue) => {
                if is_blank(queue) {
                    return Err(EnqueuePlanError::EmptyQueueName);
                }
                self.queue.clone_from(queue);
            }
            TaskOption::TaskId(id) => {
                if is_blank(id) {
                    return Err(EnqueuePlanError::EmptyTaskId);
                }
                self.task_id.clone_from(id);
            }
            TaskOption::Timeout(timeout) => {
                self.timeout = *timeout;
            }
            TaskOption::Deadline(deadline) => {
                self.deadline = Some(*deadline);
            }
            TaskOption::Unique(ttl) => {
                if *ttl < Duration::from_secs(1) {
                    return Err(EnqueuePlanError::UniqueTtlTooShort);
                }
                self.unique_ttl = Some(*ttl);
            }
            TaskOption::ProcessAt(time) => {
                self.process_at = *time;
            }
            TaskOption::ProcessIn(duration) => {
                self.process_at = now
                    .checked_add(*duration)
                    .ok_or(EnqueuePlanError::TimeOverflow("process in"))?;
            }
            TaskOption::Retention(retention) => {
                self.retention = *retention;
            }
            TaskOption::Group(group) => {
                if is_blank(group) {
                    return Err(EnqueuePlanError::EmptyGroupKey);
                }
                self.group = Some(group.clone());
            }
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), EnqueuePlanError> {
        if is_blank(&self.queue) {
            return Err(EnqueuePlanError::EmptyQueueName);
        }
        if is_blank(&self.task_id) {
            return Err(EnqueuePlanError::EmptyTaskId);
        }
        Ok(())
    }
}

fn is_blank(value: &str) -> bool {
    value.trim().is_empty()
}

impl std::fmt::Display for EnqueuePlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyTaskType => f.write_str("task typename cannot be empty"),
            Self::EmptyQueueName => f.write_str("queue name must contain one or more characters"),
            Self::EmptyTaskId => f.write_str("task ID cannot be empty"),
            Self::UniqueTtlTooShort => f.write_str("Unique TTL cannot be less than 1s"),
            Self::EmptyGroupKey => f.write_str("group key cannot be empty"),
            Self::TimeOverflow(context) => write!(f, "time overflow while computing {context}"),
        }
    }
}

impl std::error::Error for EnqueuePlanError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::UNIX_EPOCH;

    #[test]
    fn plans_default_pending_enqueue() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let task = Task::new("email:welcome", b"payload".to_vec());

        let plan = EnqueuePlan::from_task(&task, now, "generated-id").unwrap();
        let msg = plan.message();

        assert_eq!(plan.state(), TaskState::Pending);
        assert_eq!(plan.next_process_at(), Some(now));
        assert_eq!(plan.unique_lock_ttl(), None);
        assert_eq!(msg.id, "generated-id");
        assert_eq!(msg.r#type, "email:welcome");
        assert_eq!(msg.payload, b"payload");
        assert_eq!(msg.queue, keys::DEFAULT_QUEUE_NAME);
        assert_eq!(msg.retry, DEFAULT_MAX_RETRY);
        assert_eq!(msg.timeout, duration_seconds(DEFAULT_TIMEOUT));
        assert_eq!(msg.deadline, 0);
    }

    #[test]
    fn enqueue_options_override_task_options() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let task = Task::new_with_options(
            "email:welcome",
            Vec::new(),
            [
                TaskOption::queue("low"),
                TaskOption::task_id("task-option-id"),
                TaskOption::max_retry(1),
                TaskOption::timeout(Duration::from_secs(10)),
            ],
        );

        let plan = EnqueuePlan::from_task_with_options(
            &task,
            [
                TaskOption::queue("critical"),
                TaskOption::task_id("enqueue-option-id"),
                TaskOption::max_retry(3),
                TaskOption::timeout(Duration::from_secs(20)),
            ],
            now,
            "generated-id",
        )
        .unwrap();
        let msg = plan.message();

        assert_eq!(msg.queue, "critical");
        assert_eq!(msg.id, "enqueue-option-id");
        assert_eq!(msg.retry, 3);
        assert_eq!(msg.timeout, 20);
    }

    #[test]
    fn schedules_future_process_time() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let process_at = now + Duration::from_secs(60);
        let task = Task::new_with_options(
            "email:welcome",
            Vec::new(),
            [TaskOption::process_at(process_at)],
        );

        let plan = EnqueuePlan::from_task(&task, now, "generated-id").unwrap();

        assert_eq!(plan.state(), TaskState::Scheduled);
        assert_eq!(plan.next_process_at(), Some(process_at));
    }

    #[test]
    fn process_in_uses_planning_time() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let task = Task::new_with_options(
            "email:welcome",
            Vec::new(),
            [TaskOption::process_in(Duration::from_secs(90))],
        );

        let plan = EnqueuePlan::from_task(&task, now, "generated-id").unwrap();

        assert_eq!(plan.state(), TaskState::Scheduled);
        assert_eq!(plan.next_process_at(), Some(now + Duration::from_secs(90)));
    }

    #[test]
    fn sends_due_group_tasks_to_aggregating_state() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let task =
            Task::new_with_options("email:welcome", Vec::new(), [TaskOption::group("tenant-a")]);

        let plan = EnqueuePlan::from_task(&task, now, "generated-id").unwrap();
        let msg = plan.message();

        assert_eq!(plan.state(), TaskState::Aggregating);
        assert_eq!(plan.next_process_at(), None);
        assert_eq!(msg.group_key, "tenant-a");
    }

    #[test]
    fn scheduled_state_takes_priority_over_group() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let process_at = now + Duration::from_secs(60);
        let task = Task::new_with_options(
            "email:welcome",
            Vec::new(),
            [
                TaskOption::group("tenant-a"),
                TaskOption::process_at(process_at),
            ],
        );

        let plan = EnqueuePlan::from_task(&task, now, "generated-id").unwrap();

        assert_eq!(plan.state(), TaskState::Scheduled);
        assert_eq!(plan.next_process_at(), Some(process_at));
        assert_eq!(plan.message().group_key, "tenant-a");
    }

    #[test]
    fn builds_unique_key_and_lock_ttl() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let task = Task::new_with_options(
            "email:welcome",
            b"payload".to_vec(),
            [
                TaskOption::queue("critical"),
                TaskOption::unique(Duration::from_secs(300)),
            ],
        );

        let plan = EnqueuePlan::from_task(&task, now, "generated-id").unwrap();

        assert_eq!(
            plan.message().unique_key,
            "asynq:{critical}:unique:email:welcome:321c3cf486ed509164edec1e1981fec8"
        );
        assert_eq!(plan.unique_lock_ttl(), Some(Duration::from_secs(300)));
    }

    #[test]
    fn scheduled_unique_lock_ttl_covers_delay_and_uniqueness_ttl() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let task = Task::new_with_options(
            "email:welcome",
            b"payload".to_vec(),
            [
                TaskOption::process_in(Duration::from_secs(60)),
                TaskOption::unique(Duration::from_secs(300)),
            ],
        );

        let plan = EnqueuePlan::from_task(&task, now, "generated-id").unwrap();

        assert_eq!(plan.state(), TaskState::Scheduled);
        assert_eq!(plan.unique_lock_ttl(), Some(Duration::from_secs(360)));
    }

    #[test]
    fn deadline_suppresses_default_timeout() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let deadline = now + Duration::from_secs(600);
        let task = Task::new_with_options(
            "email:welcome",
            Vec::new(),
            [TaskOption::deadline(deadline)],
        );

        let plan = EnqueuePlan::from_task(&task, now, "generated-id").unwrap();

        assert_eq!(plan.message().deadline, unix_seconds(deadline));
        assert_eq!(plan.message().timeout, 0);
    }

    #[test]
    fn unix_epoch_deadline_is_still_explicit_deadline() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let task = Task::new_with_options(
            "email:welcome",
            Vec::new(),
            [TaskOption::deadline(UNIX_EPOCH)],
        );

        let plan = EnqueuePlan::from_task(&task, now, "generated-id").unwrap();

        assert_eq!(plan.message().deadline, 0);
        assert_eq!(plan.message().timeout, 0);
    }

    #[test]
    fn validates_enqueue_inputs() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

        assert_eq!(
            EnqueuePlan::from_task(&Task::new(" ", Vec::new()), now, "generated-id").unwrap_err(),
            EnqueuePlanError::EmptyTaskType
        );
        assert_eq!(
            EnqueuePlan::from_task_with_options(
                &Task::new("email:welcome", Vec::new()),
                [TaskOption::queue(" ")],
                now,
                "generated-id",
            )
            .unwrap_err(),
            EnqueuePlanError::EmptyQueueName
        );
        assert_eq!(
            EnqueuePlan::from_task_with_options(
                &Task::new("email:welcome", Vec::new()),
                [TaskOption::task_id(" ")],
                now,
                "generated-id",
            )
            .unwrap_err(),
            EnqueuePlanError::EmptyTaskId
        );
        assert_eq!(
            EnqueuePlan::from_task_with_options(
                &Task::new("email:welcome", Vec::new()),
                [TaskOption::unique(Duration::ZERO)],
                now,
                "generated-id",
            )
            .unwrap_err(),
            EnqueuePlanError::UniqueTtlTooShort
        );
        assert_eq!(
            EnqueuePlan::from_task_with_options(
                &Task::new("email:welcome", Vec::new()),
                [TaskOption::group(" ")],
                now,
                "generated-id",
            )
            .unwrap_err(),
            EnqueuePlanError::EmptyGroupKey
        );
    }
}
