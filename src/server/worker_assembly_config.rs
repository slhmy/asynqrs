//! Test-only worker-assembly construction and server-config wiring helpers.
//!
//! Reference: Asynq v0.26.0 wires processor defaults from `Config`; this crate
//! maps those defaults onto server-owned `WorkerAssembly` and `TaskExecutor`
//! pieces for retry delay, failure classification, error hooks, base context,
//! log level, logger, and janitor batch size:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L103-L130>
//! and <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L449-L603>.

mod adapters;
mod constructors;
