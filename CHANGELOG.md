# CHANGELOG

## 2026-05-15

- Added a Redis-backed async worker integration test that wires
  `AsyncRedisBroker`, `AsyncProcessor`, and `AsyncServer` together, enqueues a
  task, completes it through the async server loop, and stops through a Tokio
  watch shutdown signal.
- Reference: https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L663-L721
- TODO: Add async worker shutdown requeue and background lease extension once
  in-flight cancellation semantics are modeled.

- Added a minimal async worker processor with async broker traits, async
  handler/error-handler traits, pre-handler async lease extension, retry,
  archive, complete, idle, and maintenance paths.
- Wired `AsyncRedisBroker` into the new async worker broker traits so it can be
  used by `AsyncProcessor` and `AsyncServer`.
- Covered async processor success, retry, archive, idle, lease-extension error,
  and maintenance behavior with unit tests.
- Reference: https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L221-L381
- TODO: Add async handler panic capture, task timeout/deadline cancellation,
  background lease extension, and shutdown requeue once the runtime semantics
  are modeled.

- Added async Redis broker lease extension and active-task requeue methods:
  `AsyncRedisBroker::extend_lease_with_now` and
  `AsyncRedisBroker::requeue_with_now`.
- Covered the async lease extension and requeue paths with unit tests against
  the fake async Redis executor.
- Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go
- TODO: Wire these async lifecycle operations into a real async worker
  processor and shutdown flow.

- Added async Redis broker maintenance methods:
  `AsyncRedisBroker::forward_with_now` for scheduled/retry forwarding and
  `AsyncRedisBroker::recover_expired_leases_with_now` for lease-expired
  recovery through retry/archive.
- Covered the async forward and recover paths with unit tests against the fake
  async Redis executor.
- Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L861-L900
- Reference: https://github.com/hibiken/asynq/blob/v0.26.0/recoverer.go
- TODO: Port lease extension and shutdown requeue onto the async Redis path
  before wiring a real async processor into `AsyncServer`.

- Split Redis broker sync and async implementations into
  `src/broker/redis/broker/sync.rs` and `src/broker/redis/broker/async.rs`,
  leaving `broker.rs` as the small type/trait/shared-helper entry point.
- No public API changes; this is a maintenance-only organization pass.
- No new TODOs.

- Split Redis broker error types and lifecycle error mappings into
  `src/broker/redis/broker/error.rs`, leaving `broker.rs` focused on broker
  execution logic.
- No public API changes; this is a maintenance-only organization pass.
- No new TODOs.

- Split `src/broker/redis/broker.rs` test coverage into
  `src/broker/redis/broker/tests.rs` to keep the broker implementation file
  focused on runtime code.
- No public API changes; this is a maintenance-only organization pass.
- No new TODOs.

- Replaced hand-written error `Display`, `Error::source`, and conversion
  boilerplate across public crate errors and Redis planning/script errors with
  `thiserror` derives while preserving existing public variants and mappings.
- Reference: Rust-specific maintenance refactor; Asynq Redis behavior is
  unchanged.
- No new TODOs.

- Added the first async Redis boundary with `AsyncRedisExecutor`,
  `AsyncRedisConnectionExecutor`, and async `AsyncRedisBroker` enqueue,
  dequeue, complete, retry, and archive paths, plus unit tests for those async
  broker methods.
- Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L735
- TODO: Port lease extension and shutdown requeue onto the async Redis path
  before wiring them into the async server runtime.

- Started the async-runtime refactor by adding Tokio-native server boundaries:
  `AsyncServer`, `AsyncWorkerProcessor`, `AsyncSleeper`, `TokioSleeper`, and
  `DEFAULT_ASYNC_SERVER_IDLE_SLEEP`.
- `AsyncServer::run_until_stopped` now drives a single async worker with a
  `tokio::sync::watch` shutdown channel, while
  `AsyncServer::run_until_stopped_parallel` spawns multiple Tokio tasks and
  merges their `ServerRunSummary` values.
- Kept the existing synchronous `Server` API intact while async Redis broker
  and async `Processor` migration remain pending.
- Reference: https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L663-L721
- TODO: Port Redis execution, broker lifecycle traits, handler execution,
  lease extender, shutdown requeue, and maintenance intervals onto the async
  runtime.

- Added a first synchronous multi-worker server runner:
  `Server::run_until_stopped_parallel`.
- The parallel runner clones the processor/sleeper per worker, shares an
  `Arc<AtomicBool>` shutdown flag, joins worker threads, and merges
  `ServerRunSummary` values from each worker.
- Covered the runner with a unit test that processes two tasks across two
  worker threads and merges the completed counts.
- Reference: https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L663-L721
- TODO: Add true in-flight cancellation, shutdown requeue, background lease
  extender loops, and richer worker lifecycle coordination once the runtime
  model is expanded beyond synchronous thread workers.

- Added a synchronous server maintenance pass before each worker poll.
- `WorkerProcessor::run_maintenance` and `Processor::run_maintenance` now
  forward due scheduled/retry tasks and recover expired active-task leases for
  each configured queue, with `ServerRunSummary` tracking forwarded and
  recovered counts.
- Added a configurable pre-handler lease extension hook with
  `LeaseExtender`, `NoopLeaseExtender`, and `ExtendLeaseBeforeProcess`.
- `Processor::run_once` can now extend the lease of a dequeued task before
  running the handler, and fails fast if lease extension fails.
- Covered lease extension with processor unit tests and a Redis integration
  test.
- Covered maintenance with server unit tests and a Redis integration test where
  a due scheduled task is forwarded and processed in the same server loop.
- Updated worker lifecycle docs to describe server maintenance behavior.
- Reference: https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L687-L695
- Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L861-L900
- Reference: https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L221-L381
- Reference: https://github.com/hibiken/asynq/blob/v0.26.0/recoverer.go
- TODO: Add upstream-style independent forwarder/recoverer intervals,
  recoverer clock-skew cutoff, worker concurrency, lease extension,
  shutdown requeue, and task cancellation once async runtime semantics are
  modeled.

- Fixed non-failure retry accounting so `RedisRetryPlan` preserves `retried`
  and skips processed/failed stats when `IsFailure` returns false.
- Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L380-L418
- No new TODOs.

- Added the first synchronous worker server loop: `Server`, `Sleeper`,
  `SystemSleeper`, `ShutdownSignal`, `WorkerProcessor`, `ServerRunSummary`, and
  `ServerError`.
- `Server::run_until_stopped` now repeatedly calls a processor over configured
  queues, records completed/retried/archived/revoked/idle counts, sleeps after
  idle polls, and stops when the caller-provided shutdown signal is set.
- Covered the loop with unit tests and a Redis integration test that processes
  one successful task, retries one failed task, then sleeps on an idle poll.
- Reference: https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L663-L721
- TODO: Add worker concurrency, task context timeout/deadline handling, lease
  extension, requeue-on-shutdown, and sync retry once async/cancellation
  semantics are modeled.

- Added active-task requeue support: `RequeueBroker`, `RequeueError`,
  `RedisRequeuePlan`, `RedisBroker::requeue`, and the fixed Asynq v0.26.0
  `requeue` Lua script.
- Requeue now mirrors `RDB.Requeue` by removing the task from active and lease,
  pushing the task id back to pending, and setting task state to `pending`
  without updating processed/failed counters.
- Covered requeue behavior with Redis plan/script/broker unit tests and a Redis
  integration test.
- Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L486-L506
- TODO: Wire requeue into worker shutdown handling once `Processor` has a
  long-running server loop and cancellation model.

- Added worker error hooks: `IsFailure`, `DefaultIsFailure`, `ErrorHandler`,
  and `NoopErrorHandler`.
- `Processor::run_once` now calls the configured error handler for handler
  failures and passes the configured `IsFailure` result into retry lifecycle
  operations, matching Asynq's configurable failure-counting behavior for
  retries.
- Covered the hooks with unit tests and a Redis integration test proving a
  retried task can avoid failed counters when `IsFailure` returns false.
- Reference: https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L124-L130
- Reference: https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L277-L287
- Reference: https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L335-L360
- TODO: Add worker concurrency, task context timeout/deadline handling, lease
  extension, requeue-on-shutdown, and sync retry once the full `Server` /
  `Processor` runtime is modeled.

## 2026-05-14

- Added the first minimal worker processor: `Handler`, `HandlerError`,
  `RetryDelay`, `DefaultRetryDelay`, `Processor`, `ProcessorRun`, and
  `ProcessorError`.
- `Processor::run_once` now wires dequeue to handler execution, then routes
  success to complete, ordinary failure to retry/archive, `SkipRetry` to
  archive, and `RevokeTask` to the done/delete path.
- Covered processor behavior with unit tests and Redis integration tests for
  successful completion, handler failure retry, and retained-task revoke.
- Reference: https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L622-L650
- Reference: https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L221-L381
- TODO: Add worker concurrency, task context timeout/deadline handling, lease
  extension, requeue-on-shutdown, and sync retry once the full `Server` /
  `Processor` runtime is modeled.

- Added lease extension: `LeaseBroker`, `LeaseError`, `LeaseExtension`,
  `RedisExtendLeasePlan`, and `RedisBroker::extend_lease`.
- Redis lease extension now mirrors Asynq v0.26.0 `RDB.ExtendLease` by using
  `ZADD XX` against the queue `lease` sorted set, returning the computed new
  expiration time without creating missing lease entries.
- Covered lease extension with unit and Redis integration tests, including the
  completed-task case where extending does not recreate a lease.
- Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go
- TODO: Add the worker-side lease extender loop once `Server` / `Processor`
  handler execution is modeled.

- Added lease-expiration recovery: `RecoverBroker`, `RecoverError`,
  `RecoverResult`, `RedisRecoverPlan`, `RedisBroker::recover_expired_leases`,
  and the fixed Asynq v0.26.0 `listLeaseExpired` Lua script.
- Recovery now lists expired active-task leases and routes each task through
  retry or archive based on `retried >= retry`, updating task state, failure
  message fields, and processed/failed counters.
- Covered recover behavior with unit and Redis integration tests for expired
  active leases moving to retry/archive while active/lease entries are cleared.
- Reference: https://github.com/hibiken/asynq/blob/v0.26.0/recoverer.go
- Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go
- TODO: Add the server-side recoverer polling loop, default retry delay
  calculation, and stale aggregation-set recovery.

- Added scheduled/retry forwarding: `ForwardBroker`, `ForwardError`,
  `RedisForwardPlan`, `RedisBroker::forward_scheduled`,
  `RedisBroker::forward_retry`, and the fixed Asynq v0.26.0 `forward` Lua
  script.
- Covered forward behavior with unit and Redis integration tests for due
  scheduled/retry tasks moving back to pending, while not-yet-due tasks stay in
  their source sorted sets.
- Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L861-L900
- TODO: Add the server-side forwarder loop and lease-expiration recovery so
  scheduled/retry movement and crashed-worker recovery run automatically.

- Added the worker-side archive path: `ArchiveBroker`, `ArchiveError`,
  `RedisArchivePlan`, `RedisBroker::archive`, and the archive Lua script for
  active-to-archived failure lifecycle movement.
- Covered archive behavior with unit and Redis integration tests for active to
  archived movement, failure-message mutation, and failed statistics updates.
- Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go
- TODO: Add lease-expiration recovery and scheduled/retry forwarding next so
  failed tasks can be automatically reprocessed or recovered after worker
  crashes.

- Added the worker-side retry path: `RetryBroker`, `RetryError`,
  `RedisRetryPlan`, `RedisBroker::retry`, and the fixed Asynq v0.26.0
  `retry` Lua script.
- Covered retry behavior with unit and Redis integration tests for active to
  retry movement, retry-message mutation, and failed statistics updates.
- Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L380-L418
- TODO: Add archive and lease-expiration recovery next so the rest of the
  failure lifecycle can follow the same broker/planning pattern.

- Restructured Redis internals under `src/broker/redis/` and added a
  `broker::redis` module boundary while keeping the public crate re-exports
  stable.
- No Redis behavior changed; this is a module-organization pass to make the
  upcoming retry/archive/recovery work easier to place.

- Added the worker-side successful completion path: `CompleteBroker`,
  `CompleteError`, `RedisCompletePlan`, and `RedisBroker::complete`.
  Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L325-L379
- Added fixed Asynq v0.26.0 `done`, `done_unique`,
  `mark_as_complete`, and `mark_as_complete_unique` Lua scripts, plus
  executor support for status-returning script calls.
  Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L325-L379
- Added processed counter date keys and the 90-day stats TTL used by complete
  scripts.
  Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L54-L60
- Extended Redis integration tests to cover dequeue followed by successful
  completion for zero-retention unique tasks and retained completed tasks.
- Updated worker lifecycle docs and the enqueue tutorial to describe dequeue
  plus complete as the current successful worker path.
- TODO: Add retry, archive, lease extension, expired lease recovery, and
  completed task cleanup once failure and maintenance lifecycle paths are
  modeled.

## 2026-05-13

- Cleaned up Redis script naming now that the script catalog covers more than
  enqueue: `RedisScript` is now the public enum name.
- Renamed low-level script execution methods from enqueue/dequeue-specific
  wording to return-type-oriented `eval_script_int` and `eval_script_bytes`.
  No Redis behavior changed.
- Added the first worker-side dequeue model: `DequeuedTask`,
  `DequeueBroker`, `DequeueError`, and `RedisBroker::dequeue`.
  Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L243-L274
- Added `RedisDequeuePlan`, the fixed Asynq dequeue Lua script, executor
  support for optional message-byte script results, and Redis integration
  coverage for pending-to-active lease movement.
  Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L46-L52
- Added worker dequeue documentation and linked it from the README.
- TODO: Add ack, retry, archive, lease extension, and expired lease recovery
  once the worker lifecycle is modeled.
- Added a GitHub Actions CI workflow that runs `buf lint`,
  `cargo fmt --check`, and `cargo test` against a Redis 5.0 service via
  `ASYNQ_RS_REDIS_URL`.
  Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L735
- TODO: Broaden CI once worker-side Redis lifecycle behavior is implemented.
- Made the Redis enqueue integration tests self-contained: they now try to
  start a Redis container with testcontainers, fall back to `ASYNQ_RS_REDIS_URL`,
  and skip cleanly when Docker is unavailable.
  Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L735
- Updated the enqueue tutorial to describe the current Redis test harness and
  the `ASYNQ_RS_REDIS_URL` fallback.
- TODO: Keep CI Redis coverage enabled as worker-side lifecycle operations are
  added.

- Added `RedisConnectionProvider` and `RedisConnectionProviderExecutor`, plus
  the `RedisClientExecutor` convenience alias for enqueueing through a
  redis-rs `Client`.
  Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L735
- Updated the enqueue tutorial to reflect the current synchronous Redis client
  adapters and the remaining connection-pool gap.
- Added ignored Redis integration tests for pending, scheduled, unique, and
  group enqueue paths using `ASYNQ_RS_REDIS_URL`.
  Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L735
- TODO: Extend the automated Redis harness as non-enqueue lifecycle operations
  are added.
- Added a synchronous `RedisConnectionExecutor` adapter for the `redis` crate,
  including argument conversion for enqueue scripts.
  Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L735
- Updated the enqueue tutorial with a minimal
  `Client<RedisBroker<RedisConnectionExecutor<_>>>` wiring example.
- Added fixed Asynq v0.26.0 enqueue Lua script sources and metadata for
  script names, key counts, argument counts, and return-code semantics.
  Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L735
- Added Redis script call validation and wired it into `RedisBroker` before
  executor calls.

## 2026-05-12

- Added `RedisExecutor` and `RedisBroker` skeletons that execute
  `RedisEnqueuePlan` operations through an abstract Redis executor and map
  enqueue script return codes to `BrokerError`.
  Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L145
- TODO: Back `RedisExecutor` with a concrete Redis client and load/run the
  upstream-compatible Lua scripts.
- Updated the enqueue tutorial with the `RedisBroker` and `RedisExecutor`
  boundary.
- Added Chinese enqueue tutorial documentation covering `Task`, `TaskOption`,
  `EnqueuePlan`, `Client`, `Broker`, and `RedisEnqueuePlan`.
- Updated the Task/TaskMessage notes to reflect current option storage and the
  split between raw message construction and enqueue planning.
- Added README links to the Chinese docs.
- Added a pure Redis enqueue command plan that maps `EnqueuePlan` into
  queue-publishing and enqueue script calls for pending, scheduled,
  aggregating, and unique tasks.
  Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L6-L24
- TODO: Execute `RedisEnqueuePlan` through a real Redis client and translate
  Asynq script return codes into `BrokerError` values.
- Added a minimal `Client` and `Broker` enqueue API that builds `EnqueuePlan`,
  generates UUID task IDs by default, forwards plans to a broker, and returns
  enqueue summaries.
  Reference: https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L266-L331
- TODO: Implement a Redis-backed `Broker` that maps `EnqueuePlan` into Asynq's
  pending, scheduled, unique, and aggregation Redis operations.
- Added a pure `EnqueuePlan` model that composes task and enqueue options,
  applies upstream defaults, derives unique keys, and selects pending,
  scheduled, or aggregating state before any Redis implementation exists.
  Reference: https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L217-L331
- TODO: Wire `EnqueuePlan` into a Redis-backed broker layer.
- Added Redis key helpers for queue, task, server, scheduler, uniqueness, and
  group-related keys.
  Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L81-L139
- Applied supported `TaskOption` values when building `TaskMessage` from a
  `Task`, including queue, task id, retry count, timeout, deadline, retention,
  and group metadata.
  Reference: https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L295-L324
- TODO: Keep raw `TaskMessage::from_task` limited to message construction;
  enqueue-specific unique key and scheduling behavior now belongs to
  `EnqueuePlan`.
- Added `TaskOption` and task option storage based on Asynq v0.26.0 enqueue
  options.
  Reference: https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L47-L163
- TODO: Execute task option metadata against Redis once a Redis-backed broker is
  implemented.
- Moved task option definitions into `src/options.rs` so task data and option
  modeling stay in separate modules.
- Added project guidance in `AGENTS.md`.
- Established Asynq v0.26.0 as the current upstream reference baseline.
- Documented the requirement to add `Reference:` comments for upstream-derived
  implementation details.
- Documented the requirement to mark intentionally incomplete upstream behavior
  with nearby `TODO:` comments.
- Added this changelog as the required place to record future meaningful project
  changes.

## 2026-05-11

- Added `TaskState` based on Asynq v0.26.0 public task states.
  Reference: https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L207-L250
- Added public `Task` with type name, payload, and headers based on Asynq
  `Task`, `NewTask`, and `NewTaskWithHeaders`.
  Reference: https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L22-L73
- Marked upstream `Task` options and `ResultWriter` fields as TODOs because
  enqueue behavior and worker-side result writing are not modeled yet.
- Added Asynq `TaskMessage` protobuf schema and generated Rust bindings using
  `buf`.
  Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/proto/asynq.proto#L1-L71
