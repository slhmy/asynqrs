use thiserror::Error;

/// Error returned by a task handler.
///
/// Reference: Asynq v0.26.0 `SkipRetry`, `RevokeTask`, and panic handler
/// sentinel errors:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L327-L348>,
/// <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L633-L658>.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum HandlerError {
    #[error("{0}")]
    Failed(String),
    #[error("{0}")]
    SkipRetry(String),
    #[error("{0}")]
    RevokeTask(String),
    #[error("{0}")]
    Panic(String),
    #[error("handler not found for task")]
    HandlerNotFoundSentinel,
    #[error("handler not found for task {0:?}")]
    HandlerNotFound(String),
    #[error("asynq: task lease expired")]
    LeaseExpired,
}

impl HandlerError {
    pub fn failed(message: impl Into<String>) -> Self {
        Self::Failed(message.into())
    }

    pub fn skip_retry(message: impl Into<String>) -> Self {
        Self::SkipRetry(message.into())
    }

    pub fn revoke_task(message: impl Into<String>) -> Self {
        Self::RevokeTask(message.into())
    }

    pub fn panic(message: impl Into<String>) -> Self {
        Self::Panic(format!("panic: {}", message.into()))
    }

    pub fn panic_at(message: impl Into<String>, file: impl AsRef<str>, line: u32) -> Self {
        Self::Panic(format!(
            "panic [{}:{}]: {}",
            file.as_ref(),
            line,
            message.into()
        ))
    }

    pub fn handler_not_found(type_name: impl Into<String>) -> Self {
        Self::HandlerNotFound(type_name.into())
    }

    pub fn handler_not_found_sentinel() -> Self {
        Self::HandlerNotFoundSentinel
    }

    pub fn lease_expired() -> Self {
        Self::LeaseExpired
    }

    pub fn is_panic(&self) -> bool {
        matches!(self, Self::Panic(_))
    }

    pub fn is_skip_retry(&self) -> bool {
        matches!(self, Self::SkipRetry(_))
    }

    pub fn is_revoke_task(&self) -> bool {
        matches!(self, Self::RevokeTask(_))
    }

    pub fn is_handler_not_found(&self) -> bool {
        matches!(
            self,
            Self::HandlerNotFoundSentinel | Self::HandlerNotFound(_)
        )
    }

    pub fn is_lease_expired(&self) -> bool {
        matches!(self, Self::LeaseExpired)
    }
}

/// Reports whether a handler error is the skip-retry sentinel.
///
/// Reference: Asynq v0.26.0 public `SkipRetry` sentinel:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L327-L348>.
pub fn is_skip_retry_error(error: &HandlerError) -> bool {
    error.is_skip_retry()
}

/// Reports whether a handler error is the revoke-task sentinel.
///
/// Reference: Asynq v0.26.0 public `RevokeTask` sentinel:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L327-L348>.
pub fn is_revoke_task_error(error: &HandlerError) -> bool {
    error.is_revoke_task()
}

/// Reports whether a handler error is the lease-expired sentinel.
///
/// Reference: Asynq v0.26.0 exported `HandlerError::LeaseExpired` sentinel:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/recoverer.go#L80-L82>.
pub fn is_lease_expired_error(error: &HandlerError) -> bool {
    error.is_lease_expired()
}

/// Reports whether a handler error came from a recovered panic.
///
/// Reference: Asynq v0.26.0 public `IsPanicError`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L686-L688>.
pub fn is_panic_error(error: &HandlerError) -> bool {
    error.is_panic()
}

/// Reports whether a handler error is the servemux not-found sentinel.
///
/// Reference: Asynq v0.26.0 public `HandlerError::HandlerNotFoundSentinel`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/servemux.go#L16-L17>.
pub fn is_handler_not_found_error(error: &HandlerError) -> bool {
    error.is_handler_not_found()
}
