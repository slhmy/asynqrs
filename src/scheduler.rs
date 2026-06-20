#[cfg(test)]
use crate::server::Sleeper;
#[cfg(test)]
use crate::{BrokerError, EnqueuePlan, LogLevel, Logger, Task};
#[cfg(test)]
use chrono::Utc;
#[cfg(test)]
use std::fmt;
#[cfg(test)]
use std::time::Duration;
#[cfg(test)]
use tokio::sync::watch;

mod broker;
mod constructors;
mod core;
mod defaults;
mod entry;
mod error;
mod handle;
mod logging;
mod options;
mod registration;
mod runtime;
mod spec;
mod state;

pub use broker::SchedulerBroker;
pub use constructors::{RedisBackedScheduler, SchedulerConstructionError};
pub use core::Scheduler;
pub use defaults::{
    DEFAULT_SCHEDULER_HEARTBEAT_INTERVAL, DEFAULT_SCHEDULER_METADATA_TTL,
    DEFAULT_SCHEDULER_TICK_INTERVAL, DEFAULT_SCHEDULER_TIMEZONE,
};
pub use entry::SchedulerEntry;
pub use error::SchedulerError;
pub use handle::{SchedulerHandle, SchedulerRun};
pub use options::{
    SchedulerEnqueueErrorHook, SchedulerEnqueueHook, SchedulerEnqueuePlanErrorHook, SchedulerOpts,
    SchedulerPostEnqueueHook,
};
pub(crate) use state::SchedulerState;

#[cfg(test)]
use spec::unix_seconds;

#[cfg(test)]
mod tests;
