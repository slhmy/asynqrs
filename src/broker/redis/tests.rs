use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use super::{
    RedisBroker, RedisCancelListener, RedisConnectionExecutor, RedisScript, RedisScriptResult,
};
use crate::client::Clock;
use crate::processing::{
    DefaultIsFailure, DefaultRetryDelay, NoopErrorHandler, NoopLeaseExtender, TaskExecutor,
};
use crate::server::{
    ArchiveBroker, BorrowedWorkerFactory, Config, RetryBroker, Server, ServerClock,
    ServerConnection, ServerError, ServerHeartbeatStore, ServerLeaseExtender,
    ServerMaintenanceRunner, ServerRunSummary, ServerShutdown, ServerSyncStore, TokioSleeper,
    WorkerAssembly, WorkerRun,
};
use crate::task::TaskMessage;
use crate::{
    AdminError, ClientEnqueueScope, DequeueError, EnqueueOptions, EnqueuePlan, EnqueueResult,
    Handler, HandlerError, HandlerFunc, Pagination, ProcessingContext, ProcessingError,
    RedisBackedClient, Scheduler, SystemClock, Task, TaskState, pb,
};
use prost::Message;
use redis::Commands;
use testcontainers_modules::{
    redis::{REDIS_PORT, Redis},
    testcontainers::{Container, runners::SyncRunner},
};
use tokio::sync::{oneshot, watch};

// Reference: Asynq v0.26.0 Redis task scripts and key layout:
// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L735>.

#[path = "tests/fixture.rs"]
mod fixture;
#[path = "tests/helpers.rs"]
mod helpers;
#[path = "tests/wait.rs"]
mod wait;
use fixture::RedisFixture;
use helpers::{decode_msg, sorted, string_field, utc_date, worker_info_bytes};
use wait::wait_for_state;

async fn run_worker_once<B, H, R, C, I, E, L>(
    worker_assembly: &mut WorkerAssembly<B, H, R, C, I, E, L>,
    queues: &[String],
) -> Result<WorkerRun, ProcessingError>
where
    WorkerAssembly<B, H, R, C, I, E, L>: BorrowedWorkerFactory,
{
    let mut worker = worker_assembly.borrowed_worker();
    worker.run_once(queues).await
}

fn redis_worker_assembly<B, H>(
    broker: B,
    handler: H,
) -> WorkerAssembly<
    B,
    H,
    DefaultRetryDelay,
    SystemClock,
    DefaultIsFailure,
    NoopErrorHandler,
    NoopLeaseExtender,
> {
    WorkerAssembly::new(
        broker,
        TaskExecutor::new(handler, DefaultRetryDelay, DefaultIsFailure),
        SystemClock,
    )
}

#[path = "tests/admin.rs"]
mod admin;
#[path = "tests/aggregation.rs"]
mod aggregation;
#[path = "tests/lifecycle.rs"]
mod lifecycle;
#[path = "tests/maintenance.rs"]
mod maintenance;
#[path = "tests/metadata.rs"]
mod metadata;
#[path = "tests/result.rs"]
mod result;
#[path = "tests/scheduler.rs"]
mod scheduler;
#[path = "tests/script.rs"]
mod script;
#[path = "tests/server.rs"]
mod server;
