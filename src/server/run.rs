//! Server run entry points and top-level lifecycle orchestration.
//!
//! Reference: Asynq v0.26.0 `Server.Run` and `Server.Start` coordinate
//! workers and background lifecycle components:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L653-L721>.

mod parallel;
mod ping;
pub(in crate::server) mod runtime;
mod single;

pub use parallel::ServerHandle;
