//! Processing retry-delay and failure-classification callbacks.
//!
//! Reference: Asynq v0.26.0 public `Config.RetryDelayFunc`,
//! `Config.IsFailure`, and default callback implementations:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L119-L130>.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::{HandlerError, Task};

/// Calculates the delay before retrying a failed task.
///
/// Reference: Asynq v0.26.0 `RetryDelayFunc`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L291-L297>.
pub trait RetryDelay {
    fn retry_delay(&mut self, retried: i32, error: &HandlerError, task: &Task) -> Duration;
}

/// Function adapter for retry delay calculation.
///
/// Reference: Asynq v0.26.0 public `RetryDelayFunc` type:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L291-L297>.
pub struct RetryDelayFunc<F>(pub F);

impl<F> RetryDelayFunc<F>
where
    F: FnMut(i32, &HandlerError, &Task) -> Duration,
{
    /// Calls the wrapped retry delay function.
    ///
    /// Reference: Asynq v0.26.0 public `RetryDelayFunc` function type:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L291-L297>.
    pub fn retry_delay(&mut self, retried: i32, error: &HandlerError, task: &Task) -> Duration {
        (self.0)(retried, error, task)
    }
}

impl<F> RetryDelay for F
where
    F: FnMut(i32, &HandlerError, &Task) -> Duration,
{
    fn retry_delay(&mut self, retried: i32, error: &HandlerError, task: &Task) -> Duration {
        self(retried, error, task)
    }
}

impl<F> RetryDelay for RetryDelayFunc<F>
where
    F: FnMut(i32, &HandlerError, &Task) -> Duration,
{
    fn retry_delay(&mut self, retried: i32, error: &HandlerError, task: &Task) -> Duration {
        RetryDelayFunc::retry_delay(self, retried, error, task)
    }
}

/// Shared holder for a `Config.RetryDelayFunc` callback.
///
/// Reference: Asynq v0.26.0 public `Config.RetryDelayFunc` field:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L119-L123>.
#[derive(Clone)]
pub struct SharedRetryDelay(pub Arc<Mutex<dyn RetryDelay + Send>>);

impl std::fmt::Debug for SharedRetryDelay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SharedRetryDelay").finish_non_exhaustive()
    }
}

impl RetryDelay for SharedRetryDelay {
    fn retry_delay(&mut self, retried: i32, error: &HandlerError, task: &Task) -> Duration {
        self.0
            .lock()
            .expect("retry delay callback poisoned")
            .retry_delay(retried, error, task)
    }
}

/// Default exponential retry delay.
///
/// Reference: Asynq v0.26.0 `default_retry_delay_func` uses the Sidekiq-inspired
/// formula `n^4 + 15 + rand(0..30) * (n + 1)` seconds:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L399-L405>.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultRetryDelay;

impl DefaultRetryDelay {
    pub fn delay_for_retried_count(retried: i32) -> Duration {
        let jitter = rand::random_range(0..30_i64);
        Self::delay_for_retried_count_with_jitter(retried, jitter)
    }

    pub(crate) fn delay_for_retried_count_with_jitter(retried: i32, jitter: i64) -> Duration {
        // Reference: Asynq v0.26.0 `default_retry_delay_func` uses the supplied
        // retry count directly in Go `int` arithmetic before converting the
        // result to `time.Duration` and multiplying by `time.Second`:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L399-L405>.
        let n = i64::from(retried);
        let seconds = n
            .wrapping_mul(n)
            .wrapping_mul(n)
            .wrapping_mul(n)
            .wrapping_add(15)
            .wrapping_add(jitter.wrapping_mul(n.wrapping_add(1)));
        let nanos = seconds.wrapping_mul(1_000_000_000);
        if nanos <= 0 {
            Duration::ZERO
        } else {
            Duration::from_nanos(nanos as u64)
        }
    }
}

impl RetryDelay for DefaultRetryDelay {
    fn retry_delay(&mut self, retried: i32, _error: &HandlerError, _task: &Task) -> Duration {
        Self::delay_for_retried_count(retried)
    }
}

/// Reference: Asynq v0.26.0 public `default_retry_delay_func`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L399-L405>.
pub fn default_retry_delay_func(retried: i32, error: &HandlerError, task: &Task) -> Duration {
    DefaultRetryDelay.retry_delay(retried, error, task)
}

/// Determines whether a handler error counts as a failure in task statistics.
///
/// Reference: Asynq v0.26.0 `Config.IsFailure`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L124-L130>.
pub trait IsFailure {
    fn is_failure(&mut self, error: &HandlerError) -> bool;
}

impl<F> IsFailure for F
where
    F: FnMut(&HandlerError) -> bool,
{
    fn is_failure(&mut self, error: &HandlerError) -> bool {
        self(error)
    }
}

/// Function adapter for failure classification.
///
/// Reference: Asynq v0.26.0 public `Config.IsFailure` callback field:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L124-L130>.
pub struct IsFailureFunc<F>(pub F);

impl<F> IsFailureFunc<F>
where
    F: FnMut(&HandlerError) -> bool,
{
    /// Calls the wrapped failure predicate.
    ///
    /// Reference: Asynq v0.26.0 public `Config.IsFailure` callback field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L124-L130>.
    pub fn is_failure(&mut self, error: &HandlerError) -> bool {
        (self.0)(error)
    }
}

impl<F> IsFailure for IsFailureFunc<F>
where
    F: FnMut(&HandlerError) -> bool,
{
    fn is_failure(&mut self, error: &HandlerError) -> bool {
        IsFailureFunc::is_failure(self, error)
    }
}

/// Shared holder for a `Config.IsFailure` callback.
///
/// Reference: Asynq v0.26.0 public `Config.IsFailure` field:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L124-L130>.
#[derive(Clone)]
pub struct SharedIsFailure(pub Arc<Mutex<dyn IsFailure + Send>>);

impl std::fmt::Debug for SharedIsFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SharedIsFailure").finish_non_exhaustive()
    }
}

impl IsFailure for SharedIsFailure {
    fn is_failure(&mut self, error: &HandlerError) -> bool {
        self.0
            .lock()
            .expect("is-failure callback poisoned")
            .is_failure(error)
    }
}

/// Default failure predicate: every handler error counts as a failure.
///
/// Reference: Asynq v0.26.0 `default_is_failure_func`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L407>.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultIsFailure;

/// Reference: Asynq v0.26.0 `default_is_failure_func`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L407>.
pub fn default_is_failure_func(error: &HandlerError) -> bool {
    DefaultIsFailure.is_failure(error)
}

impl IsFailure for DefaultIsFailure {
    fn is_failure(&mut self, _error: &HandlerError) -> bool {
        true
    }
}
