# CHANGELOG

## 2026-05-13

- Added a synchronous `RedisConnectionExecutor` adapter for the `redis` crate,
  including argument conversion for enqueue scripts.
  Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L735
- Updated the enqueue tutorial with a minimal
  `Client<RedisBroker<RedisConnectionExecutor<_>>>` wiring example.
- TODO: Add real Redis integration tests for pending, scheduled, group, and
  unique enqueue paths.
- Added fixed Asynq v0.26.0 enqueue Lua script sources and metadata for
  script names, key counts, argument counts, and return-code semantics.
  Reference: https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L735
- Added Redis script call validation and wired it into `RedisBroker` before
  executor calls.
- TODO: Use the script sources in a concrete Redis executor via `EVAL` or
  `SCRIPT LOAD`/`EVALSHA`.

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
