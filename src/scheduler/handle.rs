//! Scheduler background handle and command channel types.
//!
//! Reference: Asynq v0.26.0 `Scheduler.Start`, `Scheduler.Register`, and
//! `Scheduler.Shutdown` expose a non-blocking scheduler lifecycle:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L201-L334>.

use tokio::{sync::watch, task::JoinHandle};

use super::SchedulerError;

mod command;
mod lifecycle;
mod registration;
mod run;

pub(in crate::scheduler) use command::SchedulerCommand;
pub use run::SchedulerRun;

/// Handle returned by `Scheduler::start` for a background scheduler run.
///
/// Reference: Asynq v0.26.0 `Scheduler.Start` starts the scheduler without
/// blocking the caller, while `Scheduler.Shutdown` later stops it:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L295-L334>.
#[derive(Debug)]
pub struct SchedulerHandle {
    pub(super) shutdown: watch::Sender<bool>,
    pub(super) commands: tokio::sync::mpsc::Sender<SchedulerCommand>,
    pub(super) run: JoinHandle<Result<SchedulerRun, SchedulerError>>,
}
