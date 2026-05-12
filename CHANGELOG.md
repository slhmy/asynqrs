# CHANGELOG

## 2026-05-12

- Added a pure `EnqueuePlan` model that composes task and enqueue options,
  applies upstream defaults, derives unique keys, and selects pending,
  scheduled, or aggregating state before any Redis implementation exists.
  Reference: https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L217-L331
- TODO: Wire `EnqueuePlan` into a Redis client/broker layer and replace the
  caller-supplied generated task id with a UUID generator.
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
- TODO: Apply task options to enqueue metadata once Redis enqueue behavior is
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
