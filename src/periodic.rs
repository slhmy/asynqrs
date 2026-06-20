//! Dynamic periodic task configuration.
//!
//! Reference: Asynq v0.26.0 `PeriodicTaskManager` periodically fetches task
//! configs from a provider and syncs scheduler registrations:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.

mod config;
mod constructors;
mod manager;
mod provider;

pub use config::PeriodicTaskConfig;
pub use constructors::RedisBackedPeriodicTaskManager;
pub use manager::{
    DEFAULT_PERIODIC_TASK_MANAGER_SYNC_INTERVAL, PeriodicTaskManager, PeriodicTaskManagerError,
    PeriodicTaskManagerHandle, PeriodicTaskManagerRun,
};
pub use provider::{PeriodicTaskConfigProvider, PeriodicTaskConfigProviderError};

#[cfg(test)]
mod tests;
