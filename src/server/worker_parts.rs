//! Server-side worker hot-path dependency bundles.
//!
//! Reference: Asynq v0.26.0 `Server` owns worker goroutines while processor
//! execution consumes broker, handler, retry, lease, and runtime state:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L663-L721> and
//! <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L143-L390>.
//!
//! Rust-native note: these structs make worker construction a server boundary
//! instead of an ad hoc collection of field borrows.

use std::sync::Arc;
use std::time::SystemTime;

use crate::processing::TaskExecutor;

use super::{LogLevel, Logger, ServerRuntimeState};

pub(crate) struct WorkerParts<B, H, R, C, I, E, L> {
    pub(crate) broker: B,
    pub(crate) executor: TaskExecutor<H, R, I, E, L>,
    pub(crate) clock: C,
    pub(crate) runtime: ServerRuntimeState,
    pub(crate) log_level: LogLevel,
    pub(crate) logger: Option<Arc<dyn Logger>>,
}

impl<B, H, R, C, I, E, L> WorkerParts<B, H, R, C, I, E, L> {
    pub(crate) fn new(
        broker: B,
        executor: TaskExecutor<H, R, I, E, L>,
        clock: C,
        runtime: ServerRuntimeState,
        logger: Option<Arc<dyn Logger>>,
        log_level: LogLevel,
    ) -> Self {
        Self {
            broker,
            executor,
            clock,
            runtime,
            log_level,
            logger,
        }
    }
}

pub(crate) struct BorrowedWorkerParts<'a, B, H, R, C, I, E, L> {
    pub(crate) broker: &'a mut B,
    pub(crate) executor: &'a mut TaskExecutor<H, R, I, E, L>,
    pub(crate) clock: &'a C,
    pub(crate) active_task_id: &'a mut Option<String>,
    pub(crate) runtime: &'a ServerRuntimeState,
    pub(crate) log_level: LogLevel,
    pub(crate) logger: &'a Option<Arc<dyn Logger>>,
    pub(crate) last_dequeue_error_log_at: &'a mut Option<SystemTime>,
}
