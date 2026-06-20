use std::sync::Arc;

use super::super::DEFAULT_JANITOR_BATCH_SIZE;
use super::super::Server;
use super::super::logging::{LogLevel, Logger};

impl<P, S> Server<P, S> {
    pub fn with_log_level(mut self, log_level: LogLevel) -> Self {
        self.log_level = match log_level {
            LogLevel::Unspecified => LogLevel::Info,
            log_level => log_level,
        };
        self
    }

    pub(in crate::server) fn with_optional_logger(
        mut self,
        logger: Option<Arc<dyn Logger>>,
    ) -> Self {
        self.logger = logger;
        self
    }

    pub(in crate::server) fn warn_for_large_janitor_batch_size(
        self,
        configured_batch_size: isize,
    ) -> Self {
        // Reference: Asynq v0.26.0 warns when `Config.JanitorBatchSize` is
        // greater than the recommended default batch size:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L590-L594>.
        if configured_batch_size > DEFAULT_JANITOR_BATCH_SIZE
            && matches!(
                self.log_level,
                LogLevel::Debug | LogLevel::Info | LogLevel::Warn
            )
        {
            if let Some(logger) = &self.logger {
                logger.warn(format_args!(
                    "Janitor batch size of {configured_batch_size} is greater than the recommended batch size of {DEFAULT_JANITOR_BATCH_SIZE}. \
                     This might cause a long-running script"
                ));
            }
        }
        self
    }
}
