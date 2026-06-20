use std::sync::Arc;

use crate::server::{LogLevel, Logger};

pub(super) fn log_aggregator_debug(
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    args: std::fmt::Arguments<'_>,
) {
    if matches!(log_level, LogLevel::Debug) {
        if let Some(logger) = logger {
            logger.debug(args);
        }
    }
}

pub(super) fn log_aggregator_warn(
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    args: std::fmt::Arguments<'_>,
) {
    if matches!(log_level, LogLevel::Debug | LogLevel::Info | LogLevel::Warn) {
        if let Some(logger) = logger {
            logger.warn(args);
        }
    }
}

pub(super) fn log_aggregator_error(
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    args: std::fmt::Arguments<'_>,
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
