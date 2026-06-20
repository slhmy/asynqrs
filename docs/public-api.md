# Public API Audit

This audit groups the public API by user workflow and records the Rust-first
path. Public names should use Rust conventions; wire/layout compatibility is
documented close to the implementation details that need it.

## Rust-Native Policy

- Prefer Rust-native workflow APIs over mechanical Go-shaped wrappers.
- Preserve Asynq semantics at behavior and Redis wire boundaries, not by
  mirroring Go package layout or public method spelling.
- Represent invariants with types where practical: validated queue/task/group
  names, enqueue-time options, structured errors, builder-validated config, and
  explicit runtime handles.
- Keep Go compatibility names in `Reference:` comments and migration prose when
  they explain the upstream baseline; do not add public aliases just to match Go
  unless they significantly reduce migration cost.
- Treat low-level broker, Redis script, protobuf, and runtime plumbing as
  implementation details unless exposing them enables a real Rust extension
  point.
- Crate-root exports should stay workflow-oriented. `src/lib.rs` is a public
  API facade and must not re-export crate-private internals with
  `pub(crate) use`. Implementation-shaped broker, worker, listener,
  maintenance, wire-message, time, and runtime traits should remain under their
  owning module or be crate-internal.
- Maintenance implementation constants are not public workflow API. Public
  server defaults stay limited to user-visible config intervals and typed
  runtime settings.
- Module façades should follow the same rule: only expose traits that are
  intended for user implementation. Runtime assembly traits for Redis-backed
  servers, inspectors, schedulers, aggregation, and worker loops remain
  crate-internal.

## Client and Enqueue

- Preferred constructor: `RedisBackedClient::from_redis_client`.
- Shared-client constructors: `RedisBackedClient::from_redis_runtime_client`
  and `RedisBackedClient::from_direct_redis_client`.
- Shared connection ownership affects `close` behavior but is not exposed as
  public workflow introspection.
- Preferred task flow: `Task::new` plus enqueue-time `EnqueueOptions`.
- Preferred enqueue methods: `enqueue_async`, `enqueue_with_async`, and
  `enqueue_scoped_with_async`.
- `ClientEnqueueScope` is a public Rust-native replacement for Go
  `context.Context` in enqueue operations. It carries cancellation only; task
  routing and retry behavior stay in `EnqueueOptions`.
- Enqueue planning and dequeue results are operation-boundary types exposed at
  the crate root, such as `EnqueuePlan`, `EnqueuePlanError`, `DequeuedTask`,
  and `DequeueError`; `enqueue` and `dequeue` are not public workflow modules.
- `Client` remains the explicit generic extension point for custom enqueue
  backends; low-level broker traits may live under `client`, but broker
  introspection stays test-only and those traits should not be promoted to
  crate-root workflow exports.

## Server and Handlers

- Preferred constructor: `RedisBackedServerBuilder::from_redis_client`.
- Shared-client constructors:
  `RedisBackedServerBuilder::from_redis_runtime_client` and
  `RedisBackedServerBuilder::from_direct_redis_client`.
- Advanced ownership constructor: `RedisBackedServerBuilder::new` accepts an
  explicit `RedisRuntimeClient` that the server owns. Use
  `with_shared_connection` only when constructing the same low-level boundary
  with caller-owned Redis resources.
- Preferred configuration path: `Config::builder()` plus validated builder
  setters; direct `Config` fields are internal runtime state.
- Effective server-normalization helpers stay crate-internal. Public callers can
  inspect configured values through `Config` accessors, but server construction
  owns defaulting, queue-selector normalization, and aggregation expansion.
- Preferred handler path: `ServeMux::new` plus chainable `route` / `route_fn`
  registration with `Task` and `ProcessingContext`. Mutable `handle` /
  `handle_fn` registration remains available for compatibility and tests.
- Preferred customization path: handler/middleware composition plus
  `ConfigBuilder` hooks for retry delay, failure classification, error
  handling, and processing scopes. There is no user-facing `Processor`
  construction surface; server-owned worker assembly remains internal.
- `ProcessingScope`, `ServerProcessingScope`, `ProcessingContext`, and
  `TaskMetadata` are public workflow types. `ProcessingScope` / the
  `ServerProcessingScope` callback configure the outer cancellation/deadline
  boundary for handler execution, while `ProcessingContext` / `TaskMetadata`
  are the per-attempt handler view supplied by the server.
- Preferred aggregation customization path: `ConfigBuilder::group_aggregator`
  with `GroupAggregator` / `GroupAggregatorFunc`. The lower-level aggregator
  runtime shell, aggregation run accounting, and aggregation broker remain
  server-owned internals.
- No processing runtime module is public. Handler, middleware, retry, lease,
  and error hook types are re-exported from the crate root so users can
  customize processing without depending on the internal runtime namespace.
- Preferred middleware path: chain `layer_fn` for lightweight pre-handler
  closures and `layer_hooks` / `task_middleware_hooks` when middleware needs both
  pre-handler and post-result hooks without exposing the internal `next`
  handler. Explicit `TaskMiddleware` implementations are advanced extension
  points for compatibility with the lower-level handler wrapping model.
- Preferred lifecycle path: `run` for foreground processing, `start` plus
  `ServerHandle::stop`/`shutdown`/`ping` for background processing.
- Preferred processing error path: lower-level worker processing failures use
  `ProcessingError`, and server lifecycle APIs wrap them in
  `ServerError::Processing`.
- Handler installation is typed: public Redis-backed builder methods accept a
  concrete handler, not `Option<Handler>`. Upstream nil-handler compatibility is
  retained as an internal validation branch.
- Public run observability is `ServerRunSummary`; maintenance-pass summaries
  stay internal and are folded into the run summary counters.
- `RedisBackedServer` is an opaque Redis-backed server wrapper. Its public
  methods expose lifecycle operations only; configuration projection and
  connection-ownership probes stay crate-internal test/runtime details.
- Generic server assembly (`Server<P, S>`), `TokioSleeper`, and sleeper
  injection hooks are crate-internal runtime/test surfaces. Public server
  construction should go through Redis-backed builders and typed config/handler
  customization.
- Task-only handler adapters remain Rust conveniences, but docs should prefer
  context-aware handlers because they expose cancellation, deadline, retry, and
  queue metadata.

## Scheduler and Periodic Tasks

- Preferred scheduler constructor: `RedisBackedScheduler::from_redis_client`.
- Shared-client constructors: `RedisBackedScheduler::from_redis_runtime_client`
  and `RedisBackedScheduler::from_direct_redis_client`.
- Shared connection ownership affects scheduler shutdown/close behavior but is
  not exposed as public workflow introspection.
- Preferred registration path: `register_spec_with_generated_id` or
  `register_spec_with_generated_id_and` for cron-style specs, and
  `register_with_generated_id` or `register_with_generated_id_and` for fixed
  intervals.
- Preferred lifecycle path: `run`, `start`, `shutdown`, and `ping` as the
  remaining scheduler/handle surfaces are converted.
- Scheduler configuration/state projection accessors and `SchedulerState` are
  test-only. Runtime observability for applications should go through Inspector
  scheduler metadata reads rather than direct scheduler internals.
- `Scheduler<B, C>` remains the advanced generic extension point for custom
  scheduler backends. Its required broker boundary is intentionally published as
  `scheduler::SchedulerBroker`; broker introspection stays test-only, and
  normal applications should use `RedisBackedScheduler`.
- Preferred periodic manager constructor:
  `RedisBackedPeriodicTaskManager::from_redis_client` for Redis-backed use and
  `PeriodicTaskManager::new` for explicitly assembled manager components.
  `PeriodicTaskManager::new_with_generated_scheduler_id` is available when
  assembling a manager from a custom scheduler broker.

## Inspector and Admin

- Preferred constructor: `Inspector::from_redis_client`.
- Shared-client constructors: `Inspector::from_redis_runtime_client` and
  `Inspector::from_direct_redis_client`.
- Shared connection ownership affects `close` behavior but is not exposed as
  public workflow introspection.
- Inspector custom-broker construction and broker accessors stay crate/test
  surfaces. Public applications should use Redis-backed constructors and typed
  Inspector read/mutation methods.
- Metadata wire codec helpers for server, worker, and scheduler records stay
  crate-internal. Public applications should use typed `Inspector` metadata
  methods instead of encoding or decoding Redis metadata payloads directly.
- Inspector broker capability traits live behind private modules and are not
  publicly re-exported; the release API scan guards the private Inspector
  module boundary and the crate-root `Inspector` workflow export.
- Preferred read paths: `queues`, `get_queue_info`, `list_*_tasks`,
  `get_task_info`, `servers`, `scheduler_entries`, and
  `list_scheduler_enqueue_events`.
- Preferred mutation paths: `run_task`, `archive_task`, `delete_task`,
  `delete_queue`, `pause_queue`, `unpause_queue`, and bulk state operations.
- Error behavior: public Inspector wrappers expose structured Rust errors for
  fixed validation branches.
- Public Inspector/Admin result models expose accessor methods for normal
  reads. Explicit construction uses admin-scoped input structs such as
  `ServerInfoParts`, `WorkerMetadataInfoParts`, `QueueStatsSnapshot`,
  `QueueStateSnapshot`, and `QueueThroughputSnapshot` so tests and advanced
  callers do not rely on long positional constructors. These model-construction
  helpers intentionally stay under `asynqrs::admin` rather than the crate-root
  workflow facade.

## Redis Clients and Runtime Boundary

- Preferred connection entrypoint: construct Redis clients through redis-rs and
  pass them into `from_redis_client` / `from_direct_redis_client`.
- Preferred runtime boundary: `RedisRuntimeClient`; raw `redis::Client`
  extraction is direct-only.
- Low-level Redis broker, executor, runtime connection, cancellation listener,
  aggregation broker, Inspector broker, script plans, script metadata,
  operation modules, wire task messages, and key helpers are crate-internal
  implementation details. They are not part of the workflow API.
- Worker/server/inspector/scheduler broker traits used to assemble Redis-backed
  runtimes are not re-exported from the crate root.
- Crate-private runtime helpers, protobuf task messages, clocks, Redis broker
  errors, and server lifecycle helpers are imported from their owning modules
  (`client`, `task`, `server`, `aggregation`, `scheduler`, `admin`, or
  `broker::redis`) rather than being routed through crate root.
- `Client` remains the main generic user extension point for custom enqueue
  brokers, task id generators, and clocks.
- Generated protobuf modules are crate-internal wire compatibility details.
  Public callers should use typed workflow models and Inspector methods instead
  of `pb::*` or direct metadata codec helpers.
- Dependency decisions: `WriteTimeout` is stored but not separately wired until
  redis-rs exposes a matching async runtime knob. `tls_server_name` stores the
  parsed `rediss://` server name without exposing a broader TLS config object.

## Runtime Metadata

- Preferred server metadata reads: `Inspector::servers`.
- Preferred scheduler metadata reads: `Inspector::scheduler_entries` and
  `Inspector::list_scheduler_enqueue_events`.
- Preferred active-worker source: server metadata written by the server runtime
  heartbeat.
