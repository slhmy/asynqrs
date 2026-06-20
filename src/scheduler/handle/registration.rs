//! Scheduler handle registration operations.
//!
//! Reference: Asynq v0.26.0 `Scheduler.Register` and
//! `Scheduler.Unregister` runtime mutation paths:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L201-L244>.

mod fixed;
mod spec;
mod unregister;
