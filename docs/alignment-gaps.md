# Alignment Status

This document records Asynq v0.26.0 compatibility decisions that still matter
for release review. It is not a refactor log; git history and `CHANGELOG.md`
remain the source for exact implementation details.

## P0 Correctness

### Server Runtime Ownership

- Status: Resolved for the current architecture; release verification remains.
- Completed: parallel shutdown/requeue is owned by `WorkerPool`; active worker
  registry, cancellation state, lifecycle sync backlog, lease snapshots, and
  metadata snapshots now live in server-owned `ServerRuntimeState`.
- Current boundary: spawned worker tasks run through explicit internal
  `Worker` values; before-process lease extension, handler execution, and
  complete/retry/archive report routing now flow through
  `server::runtime_parts::WorkerRuntimeParts`. Worker construction crosses
  `server::WorkerAssembly` and `server::worker_parts` owned/borrowed parts.
  The real Redis-backed server path now stores that assembly directly.
  Connection, clock, metadata heartbeat, lease extension, pending-sync store
  behavior, and active-task shutdown/requeue writes now live on
  `WorkerAssembly` or server-owned runtime helpers.
  Forwarder/recoverer/janitor maintenance behavior lives in
  `server::maintenance` for `WorkerAssembly`; shutdown/requeue and final
  pending-sync drain behavior lives in `server::shutdown` and
  `ServerRuntimeState`.
- Release review: current worker factory, shutdown, maintenance, and
  server-state capability traits are crate-internal runtime/test boundaries.
  Continue folding only traits that become pure pass-throughs. Public `Config`
  application is no longer a generic server capability; Redis-backed
  `WorkerAssembly` construction applies it directly before the runtime state is
  attached.
- Reference: Asynq v0.26.0 `server.go`, `processor.go`, `heartbeat.go`, and
  `syncer.go`.

### Pending Sync Ownership

- Status: Moved to server-owned runtime state.
- Completed: failed complete/retry/archive writes are stored in
  `PendingSyncBacklog`, a named shared owner drained through
  `ServerRuntimeState` by worker execution and the server syncer.
- Current boundary: broker-backed sync retries still run through narrow server
  store traits implemented by `WorkerAssembly`; failed worker lifecycle writes
  are enqueued by `WorkerReportRouter`.
- Deferred architectural work: keep sync storage as a server/runtime capability
  while it remains shared by worker execution, shutdown, and the server syncer.
- Reference: Asynq v0.26.0 `syncer.go`.

### Active Worker Registry and Cancellation

- Status: Moved to server-owned runtime state.
- Completed: active worker metadata, cancellation state, and pending sync are
  grouped under `ServerRuntimeState`; Redis cancellation listeners are built
  from the server runtime state after server construction.
- Current boundary: cancellation delivery targets active task ids through
  server-owned runtime state; worker-slot handles remain internal.
- Release review: introduce typed worker-slot handles only if they reduce
  concrete complexity in cancellation or active lease ownership.
- Reference: Asynq v0.26.0 `processor.go`, `heartbeat.go`, and
  `subscriber.go`.

## P1 Migration Cost

### Public Handler Compatibility

- Status: Resolved for the current public API.
- Completed: context-aware handler registration through `ServeMux::handle_fn`
  is the documented path in migration notes; task-only adapters remain
  explicit Rust compatibility helpers.
- Reference: Asynq v0.26.0 `server.go` and `servemux.go`.

### Inspector and Admin Error Branches

- Status: Resolved for implemented Inspector/Admin APIs.
- Completed: public Inspector wrappers preserve method-specific validation and
  `asynq:` error prefixes where upstream behavior differs.
- Ongoing rule: when a new Inspector/Admin method is added, add compatibility
  tests beside that method rather than relying on broad fixture assertions.
- Reference: Asynq v0.26.0 `inspector.go`.

### Scheduler and Periodic Manager

- Status: Resolved for the current typed Rust model.
- Completed: scheduler registration, hooks, metadata/history, and periodic
  manager sync lifecycle are implemented around concrete Rust config values.
- Design decision: nil task/config validation remains represented by type
  safety unless the public model intentionally grows optional upstream-shaped
  boundaries.
- Release decision: the public generic `Scheduler<B, C>` shape is an
  intentional advanced custom-broker extension point. The broker boundary is
  named as `scheduler::SchedulerBroker`, while user-facing docs should continue
  to prefer `RedisBackedScheduler`.
- Reference: Asynq v0.26.0 `scheduler.go` and `periodic_task_manager.go`.

## P2 Test Reliability

### Shutdown Time Budgets

- Status: Resolved as a project testing rule.
- Completed: Redis shutdown requeue tests configure short shutdown timeouts
  explicitly so CI does not depend on upstream's default graceful timeout.
- Rule: future shutdown/requeue integration tests must set both server shutdown
  timeout and any outer test timeout explicitly.
- Reference: Asynq v0.26.0 `server.go` and `processor.go`.

## Dependency Decisions

### Redis Write Timeout

- Status: Deferred by dependency boundary.
- Current state: Asynq-style `WriteTimeout` is stored on Redis option structs.
  Runtime clients map `DialTimeout` and `ReadTimeout` to redis-rs connection
  and response timeouts.
- Decision: do not invent a separate async write-timeout layer around redis-rs
  commands. Wire `WriteTimeout` when redis-rs exposes a matching direct,
  Sentinel, and cluster runtime option.
- Reference: Asynq v0.26.0 `asynq.go`.

### TLS Config

- Status: Deferred by public API design.
- Current state: `rediss://` parsing preserves TLS server name and direct,
  failover, and cluster options expose `TLSServerName`.
- Decision: do not add an incomplete TLS config abstraction. Introduce a
  Rust-native TLS config type only when certificate roots/client certs can be
  wired through the supported Redis runtime clients.
- Reference: Asynq v0.26.0 `asynq.go`.

## Rust Compatibility by Design

- Status: Resolved as intentional Rust API choices.
- Rust-native API and architecture are preferred whenever upstream behavior can
  be preserved without copying Go-specific shapes.
- Optional typed payload macros are Rust-native ergonomics over Asynq task type
  and payload bytes. Upstream has no equivalent derive macro; compatibility is
  preserved by producing ordinary tasks and keeping Redis payload bytes under
  user-controlled serialization.
- Go nil pointer branches are represented with `Option<&T>` encoders where the
  public compatibility function needs to preserve upstream nil errors.
- Go variadic functions are represented with iterable Rust arguments or
  separate helper functions.
- Go package-level sentinel errors are represented by Rust enum variants plus
  constructor/helper functions.
- Go string byte semantics are narrowed to UTF-8 Rust strings for queue names.
- Go package/module boundaries are not release criteria; Rust module boundaries
  should follow ownership, lifecycle, protocol, and extension-point concerns.
