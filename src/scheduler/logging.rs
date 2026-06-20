//! Scheduler logging helpers.
//!
//! Reference: Asynq v0.26.0 scheduler logs lifecycle, enqueue, shutdown, and
//! heartbeat warnings through its configured logger:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go>.

use std::fmt;
use std::sync::Arc;

use crate::{LogLevel, Logger};

pub(super) fn log_scheduler_warn(
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

pub(super) fn log_scheduler_info(
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    args: fmt::Arguments<'_>,
) {
    if matches!(log_level, LogLevel::Debug | LogLevel::Info) {
        if let Some(logger) = logger {
            logger.info(args);
        }
    }
}

pub(super) fn log_scheduler_debug(
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    args: fmt::Arguments<'_>,
) {
    if log_level == LogLevel::Debug {
        if let Some(logger) = logger {
            logger.debug(args);
        }
    }
}

pub(super) fn log_scheduler_error(
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    args: fmt::Arguments<'_>,
) {
    if matches!(
        log_level,
        LogLevel::Debug | LogLevel::Info | LogLevel::Warn | LogLevel::Error
    ) {
        if let Some(logger) = logger {
            logger.error(args);
        }
    }
}
