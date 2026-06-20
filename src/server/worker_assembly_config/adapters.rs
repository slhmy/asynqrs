use std::sync::Arc;

use crate::server::{Logger, WorkerAssembly};

use crate::processing::TaskExecutor;

impl<B, H, R, C, I, E, L> WorkerAssembly<B, H, R, C, I, E, L> {
    pub(crate) fn with_is_failure<I2>(
        self,
        is_failure: I2,
    ) -> WorkerAssembly<B, H, R, C, I2, E, L> {
        WorkerAssembly {
            broker: self.broker,
            executor: TaskExecutor {
                handler: self.executor.handler,
                retry_delay: self.executor.retry_delay,
                retry_delay_override: self.executor.retry_delay_override,
                is_failure,
                is_failure_override: self.executor.is_failure_override,
                error_handler: self.executor.error_handler,
                error_handler_override: self.executor.error_handler_override,
                lease_extender: self.executor.lease_extender,
                processing_scope: self.executor.processing_scope,
            },
            clock: self.clock,
            active_task_id: self.active_task_id,
            runtime: self.runtime,
            log_level: self.log_level,
            logger: self.logger,
            last_dequeue_error_log_at: self.last_dequeue_error_log_at,
            cleanup_batch_size: self.cleanup_batch_size,
        }
    }

    pub(crate) fn with_error_handler<E2>(
        self,
        error_handler: E2,
    ) -> WorkerAssembly<B, H, R, C, I, E2, L> {
        WorkerAssembly {
            broker: self.broker,
            executor: TaskExecutor {
                handler: self.executor.handler,
                retry_delay: self.executor.retry_delay,
                retry_delay_override: self.executor.retry_delay_override,
                is_failure: self.executor.is_failure,
                is_failure_override: self.executor.is_failure_override,
                error_handler,
                error_handler_override: self.executor.error_handler_override,
                lease_extender: self.executor.lease_extender,
                processing_scope: self.executor.processing_scope,
            },
            clock: self.clock,
            active_task_id: self.active_task_id,
            runtime: self.runtime,
            log_level: self.log_level,
            logger: self.logger,
            last_dequeue_error_log_at: self.last_dequeue_error_log_at,
            cleanup_batch_size: self.cleanup_batch_size,
        }
    }

    /// Configures the logger used by server-owned lifecycle components.
    ///
    /// Reference: Asynq v0.26.0 passes `Config.Logger` to the processor,
    /// forwarder, recoverer, and janitor:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L449-L603>.
    pub(crate) fn with_logger(mut self, logger: Arc<dyn Logger>) -> Self {
        self.logger = Some(logger);
        self
    }

    pub(crate) fn cleanup_batch_size(&self) -> isize {
        self.cleanup_batch_size
    }
}
