use std::fmt;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use crate::client::Clock;
use crate::processing::{
    DefaultIsFailure, DefaultRetryDelay, ErrorHandler, Handler, HandlerError, ProcessingError,
    ProcessingLease, RetryDelay,
};
use crate::server::{
    BorrowedWorkerFactory, DEFAULT_JANITOR_BATCH_SIZE, ServerMaintenanceRun, ServerShutdown,
    WorkerAssembly, WorkerRun, worker_task_deadline as task_deadline,
};
use crate::task::TaskMessage;
use crate::{
    AggregationError, ArchiveError, CleanupError, CompleteError, Config, DEFAULT_TIMEOUT,
    DequeueError, DequeuedTask, ErrorHandlerFunc, ExtendLeaseBeforeProcess,
    ExtendLeaseWhileProcessing, ForwardError, HandlerFunc, LeaseError, LeaseExtension, LogLevel,
    Logger, MetadataError, NoopErrorHandler, ProcessingContext, ProcessingScope, RecoverError,
    RequeueError, ResultError, RetryDelayFunc, RetryError, ServerCanceller, ServerMetadata,
    SharedErrorHandler, SharedIsFailure, SharedRetryDelay, SystemClock, Task,
    is_lease_expired_error, is_panic_error, is_revoke_task_error, is_skip_retry_error,
    lease_expired_error,
};

mod config;
mod dequeue;
mod fixtures;
mod lease_extender;
mod maintenance;
mod metadata;
mod shutdown;
mod task_execution;
mod timeout_deadline;

use fixtures::*;

async fn run_worker_once<B, H, R, C, I, E, L>(
    runtime: &mut WorkerAssembly<B, H, R, C, I, E, L>,
    queues: &[String],
) -> Result<WorkerRun, ProcessingError>
where
    WorkerAssembly<B, H, R, C, I, E, L>: BorrowedWorkerFactory,
{
    let mut worker = runtime.borrowed_worker();
    worker.run_once(queues).await
}

fn start_active_test_worker<B, H, R, C, I, E, L>(
    runtime: &mut WorkerAssembly<B, H, R, C, I, E, L>,
    message: TaskMessage,
    started_at: SystemTime,
    deadline: Option<SystemTime>,
    lease_expires_at: SystemTime,
) {
    runtime.start_active_worker(
        &message,
        started_at,
        deadline,
        ProcessingLease::new(lease_expires_at),
    );
}
