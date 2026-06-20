use std::hash::{Hash, Hasher};

use crate::{EnqueueOptions, Task};

/// Task configuration returned by a periodic task provider.
///
/// Reference: Asynq v0.26.0 `PeriodicTaskConfig` stores a cron spec, task,
/// and enqueue options for dynamic scheduler registration:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeriodicTaskConfig {
    cronspec: String,
    task: Task,
    options: EnqueueOptions,
}

impl PeriodicTaskConfig {
    pub fn new(cronspec: impl Into<String>, task: Task, options: EnqueueOptions) -> Self {
        Self {
            cronspec: cronspec.into(),
            task,
            options,
        }
    }

    pub fn cronspec(&self) -> &str {
        &self.cronspec
    }

    /// Returns mutable access to the cron spec.
    ///
    /// Reference: Asynq v0.26.0 public `PeriodicTaskConfig.Cronspec` is an
    /// exported string field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go#L73-L77>.
    pub fn cronspec_mut(&mut self) -> &mut String {
        &mut self.cronspec
    }

    pub fn task(&self) -> &Task {
        &self.task
    }

    /// Returns mutable access to the periodic task.
    ///
    /// Reference: Asynq v0.26.0 public `PeriodicTaskConfig.Task` is an
    /// exported task pointer field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go#L73-L77>.
    pub fn task_mut(&mut self) -> &mut Task {
        &mut self.task
    }

    pub fn options(&self) -> &EnqueueOptions {
        &self.options
    }

    pub fn options_mut(&mut self) -> &mut EnqueueOptions {
        &mut self.options
    }

    pub(super) fn key(&self) -> PeriodicTaskConfigKey {
        let mut options = self.options.scheduler_metadata();
        // Reference: Asynq v0.26.0 hashes periodic task configs from the
        // cronspec, task type, payload, and sorted option strings. Task
        // headers are intentionally not part of the upstream key.
        // <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
        options.sort();
        PeriodicTaskConfigKey {
            cronspec: self.cronspec.clone(),
            task_type: self.task.type_name().to_owned(),
            payload: self.task.payload().to_vec(),
            options,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PeriodicTaskConfigKey {
    cronspec: String,
    task_type: String,
    payload: Vec<u8>,
    options: Vec<String>,
}

impl Hash for PeriodicTaskConfigKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.cronspec.hash(state);
        self.task_type.hash(state);
        self.payload.hash(state);
        self.options.hash(state);
    }
}
