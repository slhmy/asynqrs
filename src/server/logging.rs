use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;

/// Server log severity level.
///
/// Reference: Asynq v0.26.0 public `LogLevel` constants and flag parsing:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L348-L390>.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LogLevel {
    Unspecified = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
    Fatal = 5,
}

/// Logger interface matching Asynq's public logger boundary.
///
/// Reference: Asynq v0.26.0 public `Logger` interface:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L333-L346>.
pub trait Logger: Send + Sync {
    fn debug(&self, args: fmt::Arguments<'_>);

    fn info(&self, args: fmt::Arguments<'_>);

    fn warn(&self, args: fmt::Arguments<'_>);

    fn error(&self, args: fmt::Arguments<'_>);

    fn fatal(&self, args: fmt::Arguments<'_>);
}

/// Reference: Asynq v0.26.0 processor dequeue error log limiter:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L107>.
pub(crate) const PROCESSING_DEQUEUE_ERROR_LOG_INTERVAL: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("asynq: unsupported log level {value:?}")]
pub struct ParseLogLevelError {
    value: String,
}

impl ParseLogLevelError {
    pub fn value(&self) -> &str {
        &self.value
    }
}

impl LogLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => panic!("asynq: unexpected log level: {}", self as i32),
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
            Self::Fatal => "fatal",
        }
    }
}

impl fmt::Display for LogLevel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for LogLevel {
    type Err = ParseLogLevelError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "debug" => Ok(Self::Debug),
            "info" => Ok(Self::Info),
            "warn" | "warning" => Ok(Self::Warn),
            "error" => Ok(Self::Error),
            "fatal" => Ok(Self::Fatal),
            _ => Err(ParseLogLevelError {
                value: value.to_owned(),
            }),
        }
    }
}

pub(super) fn log_info(
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    message: &'static str,
) {
    // Reference: Asynq v0.26.0 logs server start and shutdown lifecycle
    // messages at info level:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L672-L768>.
    if matches!(log_level, LogLevel::Debug | LogLevel::Info) {
        if let Some(logger) = logger {
            logger.info(format_args!("{message}"));
        }
    }
}

pub(super) fn log_debug(
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    message: &'static str,
) {
    // Reference: Asynq v0.26.0 server-owned aggregator shutdown logs before
    // signaling the aggregator goroutine to stop:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/aggregator.go#L58-L64>.
    if matches!(log_level, LogLevel::Debug) {
        if let Some(logger) = logger {
            logger.debug(format_args!("{message}"));
        }
    }
}

pub(super) fn log_warn(
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    args: fmt::Arguments<'_>,
) {
    if matches!(log_level, LogLevel::Debug | LogLevel::Info | LogLevel::Warn) {
        if let Some(logger) = logger {
            logger.warn(args);
        }
    }
}

pub(super) fn log_error(
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    args: fmt::Arguments<'_>,
) {
    // Reference: Asynq v0.26.0 heartbeater logs metadata write and clear
    // failures at error level:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/heartbeat.go#L88-L100>,
    // <https://github.com/hibiken/asynq/blob/v0.26.0/heartbeat.go#L143-L154>.
    if matches!(
        log_level,
        LogLevel::Debug | LogLevel::Info | LogLevel::Warn | LogLevel::Error
    ) {
        if let Some(logger) = logger {
            logger.error(args);
        }
    }
}

pub(crate) fn log_processing_warn(
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    args: fmt::Arguments<'_>,
) {
    log_warn(logger, log_level, args);
}

pub(crate) fn log_processing_debug(
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    args: fmt::Arguments<'_>,
) {
    if matches!(log_level, LogLevel::Debug) {
        if let Some(logger) = logger {
            logger.debug(args);
        }
    }
}

pub(crate) fn log_processing_error(
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    args: fmt::Arguments<'_>,
) {
    log_error(logger, log_level, args);
}
