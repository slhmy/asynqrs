//! Aggregation set, group configuration, and run summary models.
//!
//! Reference: Asynq v0.26.0 aggregator operates on queue/group aggregation
//! sets and reports checked, aggregated, reclaimed, and skipped work while
//! applying configured grace period, max delay, and max size values:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/aggregator.go>.

use std::time::{Duration, SystemTime};

use crate::Task;
use crate::task::TaskMessage;

use super::AggregationError;

#[derive(Debug, Clone, PartialEq)]
pub struct AggregationSet {
    pub(crate) messages: Vec<TaskMessage>,
    deadline: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregationGroup {
    pub(super) queue: String,
    pub(super) group: String,
    pub(super) grace_period: Duration,
    pub(super) max_delay: Duration,
    pub(super) max_size: isize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregationGroupConfig {
    pub(super) queue: String,
    pub(super) grace_period: Duration,
    pub(super) max_delay: Duration,
    pub(super) max_size: isize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct AggregatorRun {
    pub(super) checked: usize,
    pub(super) aggregated: usize,
    pub(super) reclaimed: usize,
    pub(super) skipped: usize,
}

impl AggregationSet {
    pub(crate) fn new(messages: Vec<TaskMessage>, deadline: SystemTime) -> Self {
        Self { messages, deadline }
    }

    pub fn into_tasks(self) -> Vec<Task> {
        self.messages
            .into_iter()
            .map(|message| {
                // Reference: Asynq v0.26.0 aggregator rebuilds grouped
                // messages with `NewTaskWithHeaders`, preserving type,
                // payload, and headers only.
                Task::with_headers(message.r#type, message.payload, message.headers)
            })
            .collect()
    }

    pub fn deadline(&self) -> SystemTime {
        self.deadline
    }
}

impl AggregationGroup {
    pub fn new(
        queue: impl Into<String>,
        group: impl Into<String>,
        grace_period: Duration,
        max_delay: Duration,
        max_size: isize,
    ) -> Result<Self, AggregationError> {
        let queue = queue.into();
        if queue.trim().is_empty() {
            return Err(AggregationError::EmptyQueueName);
        }
        let group = group.into();
        if group.trim().is_empty() {
            return Err(AggregationError::EmptyGroupName);
        }
        Ok(Self {
            queue,
            group,
            grace_period,
            max_delay,
            max_size,
        })
    }

    pub fn queue(&self) -> &str {
        &self.queue
    }

    pub fn group(&self) -> &str {
        &self.group
    }

    pub fn grace_period(&self) -> Duration {
        self.grace_period
    }

    pub fn max_delay(&self) -> Duration {
        self.max_delay
    }

    pub fn max_size(&self) -> isize {
        self.max_size
    }
}

impl AggregationGroupConfig {
    /// Configures dynamic aggregation-group discovery for a queue.
    ///
    /// Reference: Asynq v0.26.0 aggregator receives groups discovered from
    /// queue state and applies configured group aggregation settings:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/aggregator.go>.
    pub fn new(
        queue: impl Into<String>,
        grace_period: Duration,
        max_delay: Duration,
        max_size: isize,
    ) -> Result<Self, AggregationError> {
        let queue = queue.into();
        if queue.trim().is_empty() {
            return Err(AggregationError::EmptyQueueName);
        }
        Ok(Self {
            queue,
            grace_period,
            max_delay,
            max_size,
        })
    }

    pub fn queue(&self) -> &str {
        &self.queue
    }

    pub fn grace_period(&self) -> Duration {
        self.grace_period
    }

    pub fn max_delay(&self) -> Duration {
        self.max_delay
    }

    pub fn max_size(&self) -> isize {
        self.max_size
    }

    pub(super) fn group(&self, group: String) -> Result<AggregationGroup, AggregationError> {
        AggregationGroup::new(
            self.queue.clone(),
            group,
            self.grace_period,
            self.max_delay,
            self.max_size,
        )
    }
}

impl AggregatorRun {
    pub(super) fn merge(&mut self, other: Self) {
        self.checked += other.checked;
        self.aggregated += other.aggregated;
        self.reclaimed += other.reclaimed;
        self.skipped += other.skipped;
    }
}
