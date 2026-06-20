# Rust API Notes

This guide shows the preferred Rust-first API for common task queue workflows.
Low-level wire compatibility details stay inside the crate; application code
should use the workflow types shown here.

## Enqueue a Task

Compiled example: [`examples/enqueue.rs`](../examples/enqueue.rs).

```rust
use std::time::Duration;

use asynqrs::{EnqueueOptions, QueueName, RedisBackedClient, Task};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let redis = redis::Client::open("redis://127.0.0.1:6379/0")?;
let mut client = RedisBackedClient::from_redis_client(redis).await?;

let task = Task::new("email:welcome", br#"{"user_id":42}"#.to_vec());
let info = client
    .enqueue_with_async(
        &task,
        EnqueueOptions::new()
            .queue(QueueName::new("emails")?)
            .max_retries(5)
            .timeout(Duration::from_secs(30))
            .unique_for(Duration::from_secs(60)),
    )
    .await?;

println!("queued {} in {}", info.id(), info.queue());
client.close()?;
# Ok(())
# }
```

Reference: Asynq v0.26.0 `Client.Enqueue`.

## Run a Server

Compiled example: [`examples/server.rs`](../examples/server.rs).

```rust
use std::time::Duration;

use asynqrs::{
    Config, HandlerError, ProcessingContext, RedisBackedServerBuilder, ServeMux, Task,
};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let redis = redis::Client::open("redis://127.0.0.1:6379/0")?;

let server = RedisBackedServerBuilder::from_redis_client(
    redis,
    Config::builder()
        .concurrency(4)
        .try_queue("emails", 1usize)?
        .shutdown_timeout(Duration::from_secs(10))
        .try_build()?,
);

let mux = ServeMux::new().route_fn(
    "email:welcome",
    |task: &Task, context: &ProcessingContext| -> Result<(), HandlerError> {
        println!(
            "queue={} type={}",
            context.queue_name(),
            task.task_type()
        );
        Ok(())
    },
);

let summary = server.run(mux).await?;
println!("processed={}", summary.processed());
# Ok(())
# }
```

Reference: Asynq v0.26.0 server run and mux handler behavior.

## Add Task Middleware

Compiled example: [`examples/middleware.rs`](../examples/middleware.rs).

Use `ServeMux::layer_fn` for pre-handler middleware and `ServeMux::layer_hooks`
when middleware needs both pre-handler and post-result hooks. This is the
Rust-native customization path instead of constructing a processor runtime.

```rust
use asynqrs::{HandlerError, ProcessingContext, ServeMux, Task};

let mux = ServeMux::new()
    .layer_hooks(
        |task: &Task, context: &ProcessingContext| -> Result<(), HandlerError> {
            println!("starting {} from {}", task.task_type(), context.queue_name());
            Ok(())
        },
        |task: &Task, _context: &ProcessingContext, result| {
            if result.is_err() {
                eprintln!("task failed: {}", task.task_type());
            }
            result
        },
    )
    .route_fn(
        "email:welcome",
        |_task: &Task, _context: &ProcessingContext| -> Result<(), HandlerError> {
            Ok(())
        },
    );
```

Reference: Asynq v0.26.0 public `MiddlewareFunc`.

## Retry or Archive on Handler Failure

Returning `HandlerError::failed` follows the normal retry/archive path. The
retry delay, maximum retry count, retention, and queue options are still set on
the task/enqueue side.

Compiled example: [`examples/handler_failure.rs`](../examples/handler_failure.rs).

```rust
use asynqrs::{HandlerError, ProcessingContext, Task};

fn handler(_task: &Task, _context: &ProcessingContext) -> Result<(), HandlerError> {
    Err(HandlerError::failed("temporary provider error"))
}
```

Use `HandlerError::skip_retry()` to archive immediately and
`HandlerError::revoke_task()` to mark a task as revoked.

## Graceful Shutdown

`ServerHandle::stop` stops polling new tasks and lets background components
keep running. `ServerHandle::shutdown` starts graceful shutdown and requeues
active tasks if the configured `shutdown_timeout` expires.

Compiled example:
[`examples/graceful_shutdown.rs`](../examples/graceful_shutdown.rs).

```rust
# use asynqrs::ServerHandle;
# async fn example(handle: ServerHandle) -> Result<(), Box<dyn std::error::Error>> {
handle.stop().await?;
let summary = handle.shutdown().await?;
println!("completed={}", summary.completed());
# Ok(())
# }
```

Reference: Asynq v0.26.0 `Server.Stop` and `Server.Shutdown`.

## Schedule a Task

Compiled example: [`examples/scheduler.rs`](../examples/scheduler.rs).

Use `RedisBackedScheduler` with typed `SchedulerOpts` and Rust enqueue options
instead of upstream-shaped option structs.

```rust
use asynqrs::{EnqueueOptions, QueueName, RedisBackedScheduler, SchedulerOpts, Task};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let redis = redis::Client::open("redis://127.0.0.1:6379/0")?;
let mut scheduler =
    RedisBackedScheduler::from_redis_client(redis, SchedulerOpts::default()).await?;

let entry_id = scheduler.register_spec_with_generated_id_and(
    Task::new("email:digest", b"{}".to_vec()),
    "@every 1m",
    EnqueueOptions::new().queue(QueueName::new("emails")?),
)?;
println!("registered {entry_id}");
# Ok(())
# }
```

Reference: Asynq v0.26.0 scheduler registration.

## Inspect Runtime Metadata

Compiled example:
[`examples/inspector_metadata.rs`](../examples/inspector_metadata.rs).

Use `Inspector` typed read methods for queues, tasks, servers, workers, and
scheduler metadata. Low-level metadata codecs are crate-internal
wire-compatibility details; applications should not encode or decode Redis
metadata payloads directly.

```rust
use asynqrs::Inspector;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let redis = redis::Client::open("redis://127.0.0.1:6379/0")?;
let mut inspector = Inspector::from_redis_client(redis).await?;
let servers = inspector.servers().await?;
println!("servers={}", servers.len());
inspector.close()?;
# Ok(())
# }
```

Reference: Asynq v0.26.0 public `Inspector`.

## Aggregate Grouped Tasks

Compiled example: [`examples/aggregation.rs`](../examples/aggregation.rs).

Use `GroupAggregator` or `GroupAggregatorFunc` for Rust-native aggregation
customization. The server owns the lower-level aggregation runtime.

```rust
use asynqrs::{GroupAggregator, GroupAggregatorFunc, Task};

let mut aggregator = GroupAggregatorFunc(|group: &str, tasks: Vec<Task>| {
    let payload = format!(r#"{{"group":"{group}","count":{}}}"#, tasks.len());
    Task::new("batch:group", payload.into_bytes())
});

let task = aggregator.aggregate("tenant-a", vec![Task::new("email:welcome", b"{}".to_vec())]);
println!("aggregated type={}", task.task_type());
```

Reference: Asynq v0.26.0 group aggregation.

## Redis Clients

- Build direct Redis, Sentinel failover, and Redis Cluster clients with
  redis-rs, then pass them into the Redis-backed constructors.
- asynqrs keeps Redis connection setup at the redis-rs boundary instead of
  exposing a project-specific URI parser.
- Prefer Rust-native constructors that take Redis clients, providers, and typed
  option values directly, instead of upstream-shaped option structs that encode
  required values as nullable fields.

See [alignment-gaps.md](./alignment-gaps.md) for dependency blockers and
remaining release-readiness decisions around the server-owned runtime.
