use std::fmt;

use thiserror::Error;

use super::PeriodicTaskConfig;

/// Source of dynamic periodic task configurations.
///
/// Reference: Asynq v0.26.0 `PeriodicTaskConfigProvider` supplies configs
/// to `PeriodicTaskManager` on each sync interval:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
pub trait PeriodicTaskConfigProvider {
    fn get_configs(&mut self) -> Result<Vec<PeriodicTaskConfig>, PeriodicTaskConfigProviderError>;
}

impl<F, E> PeriodicTaskConfigProvider for F
where
    F: FnMut() -> Result<Vec<PeriodicTaskConfig>, E>,
    E: fmt::Display,
{
    fn get_configs(&mut self) -> Result<Vec<PeriodicTaskConfig>, PeriodicTaskConfigProviderError> {
        self().map_err(|error| PeriodicTaskConfigProviderError::Other(error.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PeriodicTaskConfigProviderError {
    #[error("{0}")]
    Other(String),
}
