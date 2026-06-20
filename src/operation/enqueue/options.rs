use std::time::{Duration, SystemTime};

use crate::task::validate_queue_name;
use crate::{DEFAULT_QUEUE_NAME, GroupName, QueueName, TaskId};

use crate::compat::MAX_DURATION_NANOS_U128;

use super::{DEFAULT_MAX_RETRY, EnqueuePlanError};

/// Enqueue-time task configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EnqueueOptions {
    pub(crate) max_retries: Option<u64>,
    pub(crate) queue: Option<String>,
    pub(crate) task_id: Option<String>,
    pub(crate) timeout: Option<Duration>,
    pub(crate) deadline: Option<SystemTime>,
    pub(crate) unique_for: Option<Duration>,
    pub(crate) process_at: Option<SystemTime>,
    pub(crate) process_in: Option<Duration>,
    pub(crate) retain_for: Option<Duration>,
    pub(crate) group: Option<String>,
}

impl EnqueueOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn max_retries(mut self, max_retries: u64) -> Self {
        self.max_retries = Some(max_retries);
        self
    }

    pub fn queue(mut self, queue: QueueName) -> Self {
        self.queue = Some(queue.into_string());
        self
    }

    pub fn task_id(mut self, task_id: TaskId) -> Self {
        self.task_id = Some(task_id.into_string());
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn deadline(mut self, deadline: SystemTime) -> Self {
        self.deadline = Some(deadline);
        self
    }

    pub fn unique_for(mut self, ttl: Duration) -> Self {
        self.unique_for = Some(ttl);
        self
    }

    pub fn process_at(mut self, time: SystemTime) -> Self {
        self.process_at = Some(time);
        self
    }

    pub fn process_in(mut self, duration: Duration) -> Self {
        self.process_in = Some(duration);
        self
    }

    pub fn retain_for(mut self, duration: Duration) -> Self {
        self.retain_for = Some(duration);
        self
    }

    pub fn group(mut self, group: GroupName) -> Self {
        self.group = Some(group.into_string());
        self
    }

    pub(crate) fn scheduler_metadata(&self) -> Vec<String> {
        let mut options = Vec::new();
        if let Some(queue) = &self.queue {
            options.push(format!("Queue({})", quote_go_string(queue)));
        }
        if let Some(task_id) = &self.task_id {
            options.push(format!("TaskID({})", quote_go_string(task_id)));
        }
        if let Some(retry) = self.max_retries {
            options.push(format!("MaxRetry({retry})"));
        }
        if let Some(timeout) = self.timeout {
            options.push(format!("Timeout({})", display_duration(timeout)));
        }
        if let Some(deadline) = self.deadline {
            options.push(format!("Deadline({})", display_unix_date(deadline)));
        }
        if let Some(ttl) = self.unique_for {
            options.push(format!("Unique({})", display_duration(ttl)));
        }
        if let Some(time) = self.process_at {
            options.push(format!("ProcessAt({})", display_unix_date(time)));
        }
        if let Some(duration) = self.process_in {
            options.push(format!("ProcessIn({})", display_duration(duration)));
        }
        if let Some(duration) = self.retain_for {
            options.push(format!("Retention({})", display_duration(duration)));
        }
        if let Some(group) = &self.group {
            options.push(format!("Group({})", quote_go_string(group)));
        }
        options
    }
}

fn quote_go_string(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for ch in value.chars() {
        match ch {
            '\u{07}' => quoted.push_str("\\a"),
            '\u{08}' => quoted.push_str("\\b"),
            '\u{0C}' => quoted.push_str("\\f"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            '\u{0B}' => quoted.push_str("\\v"),
            '\\' => quoted.push_str("\\\\"),
            '"' => quoted.push_str("\\\""),
            '\0'..='\u{1F}' | '\u{7F}' => {
                quoted.push_str(&format!("\\x{:02x}", ch as u32));
            }
            '\u{2028}' | '\u{2029}' => {
                quoted.push_str(&format!("\\u{:04x}", ch as u32));
            }
            _ if ch.is_control() && (ch as u32) <= 0xFFFF => {
                quoted.push_str(&format!("\\u{:04x}", ch as u32));
            }
            _ if ch.is_control() => {
                quoted.push_str(&format!("\\U{:08x}", ch as u32));
            }
            _ => quoted.push(ch),
        }
    }
    quoted.push('"');
    quoted
}

fn display_duration(duration: Duration) -> String {
    if duration.is_zero() {
        return "0s".to_owned();
    }

    let total_seconds = duration.as_secs();
    let nanos = duration.subsec_nanos();
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;

    if hours > 0 {
        return format!(
            "{hours}h{minutes}m{}",
            display_fractional_unit(seconds as u128, nanos as u128, 1_000_000_000, "s")
        );
    }
    if minutes > 0 {
        return format!(
            "{minutes}m{}",
            display_fractional_unit(seconds as u128, nanos as u128, 1_000_000_000, "s")
        );
    }
    if total_seconds > 0 {
        return display_fractional_unit(total_seconds as u128, nanos as u128, 1_000_000_000, "s");
    }

    let nanos = duration.as_nanos();
    if nanos >= 1_000_000 {
        return display_fractional_unit(nanos / 1_000_000, nanos % 1_000_000, 1_000_000, "ms");
    }
    if nanos >= 1_000 {
        return display_fractional_unit(nanos / 1_000, nanos % 1_000, 1_000, "µs");
    }
    format!("{nanos}ns")
}

fn display_fractional_unit(whole: u128, fraction: u128, scale: u128, unit: &str) -> String {
    if fraction == 0 {
        return format!("{whole}{unit}");
    }
    let width = scale.ilog10() as usize;
    let mut decimal = format!("{fraction:0width$}");
    while decimal.ends_with('0') {
        decimal.pop();
    }
    format!("{whole}.{decimal}{unit}")
}

fn display_unix_date(time: SystemTime) -> String {
    let datetime: chrono::DateTime<chrono::Utc> = time.into();
    datetime.format("%a %b %e %H:%M:%S UTC %Y").to_string()
}

#[derive(Debug)]
pub(super) struct ComposedOptions {
    pub(super) retry: u64,
    pub(super) queue: String,
    pub(super) task_id: String,
    pub(super) timeout: Duration,
    pub(super) deadline: Option<SystemTime>,
    pub(super) unique_ttl: Option<Duration>,
    pub(super) process_at: SystemTime,
    pub(super) retention: Duration,
    pub(super) group: Option<String>,
}

impl ComposedOptions {
    pub(super) fn new(
        now: SystemTime,
        generated_task_id: String,
    ) -> Result<Self, EnqueuePlanError> {
        if is_blank(&generated_task_id) {
            return Err(EnqueuePlanError::EmptyTaskId);
        }

        Ok(Self {
            retry: DEFAULT_MAX_RETRY as u64,
            queue: DEFAULT_QUEUE_NAME.to_owned(),
            task_id: generated_task_id,
            timeout: Duration::ZERO,
            deadline: None,
            unique_ttl: None,
            process_at: now,
            retention: Duration::ZERO,
            group: None,
        })
    }

    pub(super) fn apply_options<N>(
        &mut self,
        options: &EnqueueOptions,
        now: &mut N,
    ) -> Result<(), EnqueuePlanError>
    where
        N: FnMut() -> SystemTime,
    {
        if let Some(retry) = options.max_retries {
            self.retry = retry;
        }
        if let Some(queue) = &options.queue {
            validate_queue_name(queue).map_err(|_| EnqueuePlanError::EmptyQueueName)?;
            self.queue.clone_from(queue);
        }
        if let Some(id) = &options.task_id {
            if is_blank(id) {
                return Err(EnqueuePlanError::EmptyTaskId);
            }
            self.task_id.clone_from(id);
        }
        if let Some(timeout) = options.timeout {
            validate_go_duration(timeout, "timeout")?;
            self.timeout = timeout;
        }
        if let Some(deadline) = options.deadline {
            self.deadline = Some(deadline);
        }
        if let Some(ttl) = options.unique_for {
            validate_go_duration(ttl, "unique ttl")?;
            if ttl < Duration::from_secs(1) {
                return Err(EnqueuePlanError::UniqueTtlTooShort);
            }
            self.unique_ttl = Some(ttl);
        }
        if let Some(time) = options.process_at {
            self.process_at = time;
        }
        if let Some(duration) = options.process_in {
            validate_go_duration(duration, "process in")?;
            self.process_at = now()
                .checked_add(duration)
                .ok_or(EnqueuePlanError::TimeOverflow("process in"))?;
        }
        if let Some(retention) = options.retain_for {
            validate_go_duration(retention, "retention")?;
            self.retention = retention;
        }
        if let Some(group) = &options.group {
            if is_blank(group) {
                return Err(EnqueuePlanError::EmptyGroupKey);
            }
            self.group = Some(group.clone());
        }
        Ok(())
    }

    pub(super) fn validate(&self) -> Result<(), EnqueuePlanError> {
        validate_queue_name(&self.queue).map_err(|_| EnqueuePlanError::EmptyQueueName)?;
        if is_blank(&self.task_id) {
            return Err(EnqueuePlanError::EmptyTaskId);
        }
        Ok(())
    }
}

fn validate_go_duration(duration: Duration, field: &'static str) -> Result<(), EnqueuePlanError> {
    // Reference: Asynq v0.26.0 duration-backed options use Go
    // `time.Duration`, an `int64` nanosecond count, before being composed into
    // enqueue options and task-message seconds:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L124-L208>.
    if duration.as_nanos() > MAX_DURATION_NANOS_U128 {
        return Err(EnqueuePlanError::DurationOverflow(field));
    }
    Ok(())
}

/// Reference: Asynq v0.26.0 `is_blank` helper:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L300-L303>.
pub(super) fn is_blank(value: &str) -> bool {
    value.trim().is_empty()
}
