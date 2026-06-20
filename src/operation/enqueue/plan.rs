use std::time::{Duration, SystemTime};

use crate::broker::redis::keys;
use crate::task::TaskMessage;
use crate::task::{duration_seconds, retry_count_i32, unix_seconds};
use crate::{Task, TaskState};

use super::options::{ComposedOptions, EnqueueOptions, is_blank};
use super::{DEFAULT_TIMEOUT, EnqueuePlanError, NO_DEADLINE, NO_TIMEOUT};

/// Pure enqueue decision plan.
///
/// Reference: Asynq v0.26.0 `Client.EnqueueContext` composes options, builds
/// `base.TaskMessage`, and selects pending, scheduled, or aggregating state:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L271-L331>.
#[derive(Debug, Clone, PartialEq)]
pub struct EnqueuePlan {
    message: TaskMessage,
    max_retry: u64,
    state: TaskState,
    next_process_at: Option<SystemTime>,
    unique_lock_ttl: Option<Duration>,
}

impl EnqueuePlan {
    pub fn from_task(
        task: &Task,
        now: SystemTime,
        generated_task_id: impl Into<String>,
    ) -> Result<Self, EnqueuePlanError> {
        Self::from_task_with_options(task, EnqueueOptions::default(), now, generated_task_id)
    }

    pub fn from_task_with_options(
        task: &Task,
        options: EnqueueOptions,
        now: SystemTime,
        generated_task_id: impl Into<String>,
    ) -> Result<Self, EnqueuePlanError> {
        Self::from_task_with_options_at(task, options, now, now, now, generated_task_id)
    }

    pub(crate) fn from_task_with_options_at(
        task: &Task,
        options: EnqueueOptions,
        option_now: SystemTime,
        enqueue_now: SystemTime,
        unique_ttl_now: SystemTime,
        generated_task_id: impl Into<String>,
    ) -> Result<Self, EnqueuePlanError> {
        Self::from_task_with_options_using_clocks(
            task,
            options,
            || option_now,
            || enqueue_now,
            || unique_ttl_now,
            generated_task_id,
        )
    }

    pub(crate) fn from_task_with_options_using_clocks<O, E, U>(
        task: &Task,
        options: EnqueueOptions,
        mut option_now: O,
        mut enqueue_now: E,
        mut unique_ttl_now: U,
        generated_task_id: impl Into<String>,
    ) -> Result<Self, EnqueuePlanError>
    where
        O: FnMut() -> SystemTime,
        E: FnMut() -> SystemTime,
        U: FnMut() -> SystemTime,
    {
        // Reference: Asynq v0.26.0 `composeOptions` reads `time.Now()` for
        // the default `processAt` value and again for each `ProcessIn` option:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L217-L264>.
        if is_blank(task.type_name()) {
            return Err(EnqueuePlanError::EmptyTaskType);
        }

        let mut opts = ComposedOptions::new(option_now(), generated_task_id.into())?;
        opts.apply_options(&options, &mut option_now)?;
        opts.validate()?;

        let enqueue_now = enqueue_now();
        let deadline = opts.deadline.map(unix_seconds).unwrap_or(0);
        let mut timeout = duration_seconds(opts.timeout);
        // Reference: Asynq v0.26.0 uses `time.Unix(0, 0)` as the
        // no-deadline sentinel before applying `DEFAULT_TIMEOUT`:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L235-L239>.
        if deadline == unix_seconds(NO_DEADLINE) && opts.timeout == NO_TIMEOUT {
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
            retry: retry_count_i32(opts.retry),
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

        // Reference: Asynq v0.26.0 computes `ProcessIn` during
        // `composeOptions`, compares the resulting `processAt` against a later
        // `time.Now()` in `Client.EnqueueContext`, then computes scheduled
        // unique TTL through `time.Until` inside `Client.schedule`:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L246-L248>
        // <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L319-L329>
        // <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L432-L438>.
        let process_delay = opts.process_at.duration_since(enqueue_now).ok();
        let state = if process_delay.is_some_and(|duration| duration > Duration::ZERO) {
            TaskState::Scheduled
        } else if !message.group_key.is_empty() {
            TaskState::Aggregating
        } else {
            TaskState::Pending
        };

        let next_process_at = match state {
            TaskState::Scheduled => Some(opts.process_at),
            TaskState::Pending => Some(enqueue_now),
            TaskState::Aggregating => None,
            _ => unreachable!("enqueue only creates pending, scheduled, or aggregating tasks"),
        };

        let unique_lock_ttl = match (opts.unique_ttl, state) {
            (Some(ttl), TaskState::Scheduled) => Some(
                opts.process_at
                    .checked_add(ttl)
                    .and_then(|expires_at| expires_at.duration_since(unique_ttl_now()).ok())
                    .ok_or(EnqueuePlanError::TimeOverflow("scheduled unique ttl"))?,
            ),
            (Some(ttl), _) => Some(ttl),
            (None, _) => None,
        };

        Ok(Self {
            message,
            max_retry: opts.retry,
            state,
            next_process_at,
            unique_lock_ttl,
        })
    }

    pub(crate) fn message(&self) -> &TaskMessage {
        &self.message
    }

    pub fn max_retry(&self) -> u64 {
        self.max_retry
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
