# Rust-Native Runtime Architecture

This document describes the current Rust-native runtime ownership model. It is
not a migration log; completed refactor history lives in `CHANGELOG.md`.

## Decision

`Server` owns runtime state and lifecycle orchestration. There is no
crate-internal `Processor` shell that owns server runtime behavior.

Asynq v0.26.0 splits server lifecycle, processor execution, heartbeat, syncer,
and subscriber behavior across several Go files. This crate preserves the
public workflow and Redis wire behavior, but uses Rust ownership boundaries
instead of Go package/runtime ownership.

Reference: Asynq v0.26.0 server and processor lifecycle:
<https://github.com/hibiken/asynq/tree/v0.26.0>.

## Current Model

```text
RedisBackedServer
  owns Server
    owns runtime: WorkerAssembly
    owns ServerRuntimeState
    owns queue selection, metadata, config, listeners, and runners

ServerRuntime
  owns parallel worker orchestration
  owns background lifecycle ordering

WorkerPool
  owns spawned Worker tasks

Worker
  owns the one-task dequeue/process/report ordering

WorkerAssembly
  owns broker, TaskExecutor, clock, runtime state handle, logging, and
  maintenance/shutdown capability implementations

TaskExecutor
  owns the user handler pipeline
```

In short:

```text
Server = orchestration and lifecycle
ServerRuntimeState = shared active task/cancellation/sync/metadata state
WorkerAssembly = concrete server-owned runtime component
Worker = dequeue/process/report loop
TaskExecutor = user handler pipeline
Broker = Redis lifecycle operations
```

## Ownership Boundaries

### Server

`Server` stores the concrete runtime owner in its `runtime` field. That runtime
is currently `WorkerAssembly` for the real Redis-backed server path.

`Server` owns:

- queue selection and worker count
- stop and shutdown coordination
- metadata heartbeat configuration
- health check configuration
- maintenance intervals
- cancellation listener
- aggregation runner
- shared `ServerRuntimeState`

### ServerRuntimeState

`ServerRuntimeState` is the shared state owner for a server instance.

It owns:

- active worker registry
- cancellation registry
- pending sync backlog
- lease snapshots used by heartbeat
- metadata snapshots derived from active workers

This state lives under `server`, not under handler execution or broker
adapters.

### WorkerAssembly

`WorkerAssembly` is the concrete runtime component used by `Server`.

It owns or carries:

- Redis broker/runtime operations
- `TaskExecutor`
- clock
- `ServerRuntimeState`
- logging configuration
- cleanup batch size
- borrowed single-worker active task slot

It implements the crate-private server lifecycle capabilities for connection
control, metadata writes, lease extension, maintenance passes, pending-sync
flushes, runtime-state attachment, and shutdown. Public `Config` application is
handled directly by `WorkerAssembly` during Redis-backed server construction
rather than through a generic server capability trait.

### WorkerPool And Worker

Parallel `Server::start` uses `ServerRuntime` and `WorkerPool` to spawn
explicit `Worker` values. Single-worker `Server::run` borrows a worker handle
from `WorkerAssembly` so non-clone handlers remain supported.

`Worker` owns the order of one task:

1. drain pending sync once
2. dequeue a task
3. register active task state
4. run before-process lease setup
5. register cancellation
6. execute the task through `TaskExecutor`
7. route success or failure through `WorkerReportRouter`
8. unregister cancellation and finish active task state

### TaskExecutor

`TaskExecutor` owns the user-facing handler pipeline:

- handler or `ServeMux`
- middleware chain
- retry delay policy
- failure classifier
- error handler
- processing scope
- result writer integration

It does not own server metadata, active worker state, cancellation registries,
or pending sync.

## Internal Trait Policy

Remaining crate-private runtime traits are allowed only when they are real
component boundaries. They must not exist just to preserve a processor-shaped
compatibility layer. The current audit result is that these traits are
crate-internal runtime/test boundaries, not user extension points.

Currently retained internal boundaries:

| Boundary | Why It Remains |
| --- | --- |
| `ServerConnection` | Server lifecycle, health checks, and handle ping need ping/close without depending on worker execution. |
| `ServerClock` | Metadata heartbeat, syncer, maintenance, and shutdown need runtime time without reaching into worker handles. |
| `ServerSyncStore` | Shutdown and syncer flush pending lifecycle writes while the concrete broker remains owned by runtime components. |
| `ServerHeartbeatStore` | Server heartbeat owns when metadata is written or cleared; runtime components provide the broker write. |
| `ServerLeaseExtender` | Server-owned active-worker heartbeat owns batch selection; runtime components provide batched lease writes. |
| `ServerMaintenanceRunner` | Forwarder, recoverer, and janitor scheduling is server-owned while maintenance passes still use broker operations. |
| `ServerRuntimeStateAttach` | Construction calls `attach_server_runtime` to mutably attach one shared `ServerRuntimeState` to concrete runtimes and focused test doubles before moving the runtime owner into `Server`. |
| `ServerShutdown` | Server decides shutdown timing; runtime components drain pending sync and requeue active work they own. |
| `WorkerFactory` | Parallel `start` needs owned worker handles that can be spawned. |
| `BorrowedWorkerFactory` | Foreground `run` needs borrowed worker handles so handlers do not have to implement `Clone`. |
| `WorkerTaskRunner` | Worker pools need a narrow "run at most one task" boundary. |
| `WorkerTaskLifecycle` | `Worker` owns one-task ordering while handles provide the concrete lifecycle operations. |
| `WorkerReporter` | `Worker` owns execution order while `WorkerReportRouter` owns complete/retry/archive decisions. |
| `WorkerBrokerCore` | Worker hot-path dequeue, complete, retry, archive, and result writes use opaque futures without changing public handler traits. |
| `RetryBroker`, `ArchiveBroker`, `RequeueBroker` | Retry/archive/requeue are reused by worker reporting, maintenance recovery, pending-sync replay, and shutdown. |
| `LeaseBroker`, `ForwardBroker`, `RecoverBroker`, `CleanupBroker`, `MetadataBroker`, `PingBroker`, `AggregationBroker` | These are crate-private broker-operation boundaries for server-owned lifecycle components and Redis/test implementations; lease extension is batch-only at this boundary. Recoverer maintenance reuses `AggregationBroker` for stale aggregation reclaim instead of a server-specific alias, and aggregation group discovery is an explicit required broker capability rather than a default empty list. |

Deleted transitional worker broker facades:

- `DequeueBroker`
- `CompleteBroker`
- `ResultBroker`

Worker dequeue, complete, retry, archive, and handler result writes now use the
crate-internal `WorkerBrokerCore` hot path. `RetryBroker`, `ArchiveBroker`, and
`RequeueBroker` remain separate because maintenance and shutdown reuse those
lifecycle operations outside the hot worker loop.

## Public API Contract

Runtime cleanup should keep these public workflow surfaces stable unless the
public API audit explicitly decides otherwise:

- `RedisBackedServerBuilder`
- `RedisBackedServer`
- `ServerHandle`
- `Config` and `ConfigBuilder`
- `ServeMux`
- handler registration and middleware hooks
- retry delay, failure classification, error handler, health check, logger, and
  processing scope customization

Users should not construct a `Processor`. Server runtime assembly remains
internal.

## Remaining Release Cleanup

Release cleanup should preserve the model above and remove anything that makes
the runtime look processor-owned again:

- Continue folding capability traits only when a current audit shows they no
  longer mark a durable boundary among `WorkerAssembly`, `Worker`,
  `TaskExecutor`, runtime state, maintenance, or shutdown.
- Re-run `scripts/semantic-gap-scan.sh --self-test` and
  `scripts/semantic-gap-scan.sh` before release, and keep any remaining
  processor wording tied to upstream Asynq references or the crate-private
  customization module.
- Keep upstream `Processor` wording only where it describes Asynq references.
- Ensure README, migration docs, examples, and public API notes use the
  preferred Redis-backed server path rather than internal runtime types.
- Keep `buf lint`, `cargo fmt --check`, `cargo test --examples`,
  `cargo test`, and `git diff --check` green after each cleanup step. Release
  verification also requires the strict Redis smoke matrix from
  `docs/redis-smoke-matrix.md`.
