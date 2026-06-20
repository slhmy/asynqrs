//! Processing handler execution helpers.
//!
//! Reference: Asynq v0.26.0 processor executes handlers while watching
//! cancellation, deadlines, result writes, and lease expiry:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L221-L381>.

mod handler;
mod result;
mod task;

pub(crate) use result::{TaskExecutionContext, perform_with_lease_extender};
pub(crate) use task::task_from_message;
