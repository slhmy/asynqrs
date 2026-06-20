//! Server builder surface.
//!
//! Reference: Asynq v0.26.0 `Config` fields are normalized while wiring
//! server lifecycle components:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L431-L620>.

mod aggregation;
mod health;
mod intervals;
mod logging;
mod runtime;
