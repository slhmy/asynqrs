#[cfg(test)]
use async_trait::async_trait;

mod execution;
mod executor;
mod handler;
mod lease;
mod retry;

pub(crate) use crate::server::{ProcessingError, ProcessingLease, WorkerRun};
pub(crate) use execution::{TaskExecutionContext, perform_with_lease_extender, task_from_message};
pub(crate) use executor::TaskExecutor;
pub use handler::{
    ErrorHandler, ErrorHandlerFunc, Handler, HandlerError, HandlerFunc, NoopErrorHandler,
    NotFoundHandler, ServeMux, ServeMuxMatchedHandler, SharedErrorHandler, TaskHandlerFunc,
    TaskMiddleware, TaskMiddlewareFn, TaskMiddlewareHooks, TypedHandlerFunc,
    is_handler_not_found_error, is_lease_expired_error, is_panic_error, is_revoke_task_error,
    is_skip_retry_error, not_found, not_found_handler, task_middleware_fn, task_middleware_hooks,
    typed_handler,
};
pub use lease::{
    ExtendLeaseBeforeProcess, ExtendLeaseWhileProcessing, LeaseExtender, NoopLeaseExtender,
    lease_expired_error,
};
pub use retry::{
    DefaultIsFailure, DefaultRetryDelay, IsFailure, IsFailureFunc, RetryDelay, RetryDelayFunc,
    SharedIsFailure, SharedRetryDelay, default_is_failure_func, default_retry_delay_func,
};

#[cfg(test)]
mod tests;
