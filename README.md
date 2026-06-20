# Asynqrs

A production-shaped Redis task queue for Rust, aligned with Asynq's
battle-tested model.

Rust learning/reimplementation project for
[hibiken/asynq](https://github.com/hibiken/asynq), a Go Redis-backed task queue.

The crate preserves Asynq-compatible workflow and Redis wire behavior where
that matters, while using Rust-native APIs and ownership internally. The current
server runtime is server-owned: users build Redis-backed clients, servers,
schedulers, and inspectors through typed constructors rather than constructing a
`Processor` runtime object.

The 0.2.0 release adds optional typed-payload ergonomics on top of the
Redis-backed client, server, scheduler, inspector, and aggregation workflows.
Macro support is opt-in: default builds keep the hand-written task API and do
not pull the companion proc-macro crate or JSON serialization dependencies.

## Install

Use the core task queue API without optional macro dependencies:

```toml
[dependencies]
asynqrs = "0.2"
```

Enable the typed payload derive and JSON helpers when you want macro-powered
task definitions:

```toml
[dependencies]
asynqrs = { version = "0.2", features = ["macros", "serde"] }
serde = { version = "1", features = ["derive"] }
```

## Public Workflows

- Enqueue tasks with `RedisBackedClient`, `Task`, and `EnqueueOptions`.
- Define typed task payloads with the optional `macros` and `serde` features,
  then convert them into ordinary `Task` values.
- Process tasks with `RedisBackedServerBuilder`, `Config::builder()`, and
  `ServeMux` or a custom `Handler`.
- Run background servers with `start`, then coordinate `ServerHandle::stop`,
  `shutdown`, and `ping`.
- Register scheduled tasks with `RedisBackedScheduler`.
- Aggregate grouped tasks with `GroupAggregator` or `GroupAggregatorFunc`.
- Inspect queues, tasks, servers, workers, and scheduler metadata with
  `Inspector`.

The internal processing runtime is not a public module. Processing customization
goes through crate-root handler, middleware, retry, lease, and error-hook exports
rather than an implementation-shaped runtime namespace.

See [docs/public-api.md](docs/public-api.md) for the current preferred API
surface and [docs/migration.md](docs/migration.md) for Asynq migration notes.

## Example

Run the Redis server example in one terminal:

```sh
cargo run --example server
```

Then enqueue a task from another terminal:

```sh
cargo run --example enqueue
```

To target another Redis instance:

```sh
ASYNQ_RS_REDIS_URL=redis://127.0.0.1:6379/0 cargo run --example server
ASYNQ_RS_REDIS_URL=redis://127.0.0.1:6379/0 cargo run --example enqueue
```

Optional typed payload ergonomics are available behind feature flags:

```sh
cargo run --example typed_payload --features macros,serde
cargo run --example macro_handlers --features macros,serde
```

Manual `TypedTaskPayload` implementations can be written without macro support.
The `#[derive(TaskPayload)]` convenience path uses JSON helpers, so it requires
both `macros` and `serde`.

The derive keeps Redis wire behavior ordinary: a typed payload still becomes an
`asynqrs::Task` with a task type string and payload bytes. Handler adapters and
`serve_mux!` reduce repeated task type and decode boilerplate without replacing
`ServeMux`, `Handler`, or `Task::new`.

The release-facing docs are intentionally small:

- [docs/public-api.md](docs/public-api.md): preferred user workflows and public
  extension points.
- [docs/migration.md](docs/migration.md): Rust-first migration notes for Asynq
  users.
- [docs/alignment-gaps.md](docs/alignment-gaps.md): compatibility decisions and
  deferred dependency gaps.
- [docs/rust-native-runtime-redesign.md](docs/rust-native-runtime-redesign.md):
  current server-owned runtime architecture.
- [docs/redis-smoke-matrix.md](docs/redis-smoke-matrix.md): Redis lifecycle
  scenario coverage.
- [docs/release-readiness-roadmap.md](docs/release-readiness-roadmap.md): final
  release decision, evidence checklist, and current audit.

## Development Verification

After code, schema, or public documentation changes, run:

```sh
scripts/release-gate.sh
```

The Redis smoke command requires either `ASYNQ_RS_REDIS_URL` set to the Redis
instance that strict smoke should verify or a working Docker daemon for
`testcontainers`.
GitHub CI uses the Redis service-container path with `ASYNQ_RS_REDIS_URL`
instead of Docker-in-Docker; the Docker-backed `testcontainers` path is for
local release verification when no Redis URL is supplied.
`scripts/redis-smoke-preflight.sh` runs before the strict Redis smoke command
and fails early when neither Redis URL nor Docker daemon access is available; it
does not replace the strict smoke command's Redis reachability check.
The release gate also runs `scripts/public-api-scan.sh` to catch accidental
public re-exports of internal runtime/state types and
`scripts/semantic-gap-scan.sh` to catch scattered TODO/FIXME or known-gap
markers outside the alignment docs.
`scripts/docs-set-scan.sh` keeps the docs directory limited to the release-facing
document set listed above. `scripts/release-metadata-scan.sh` keeps
Cargo package metadata, Rust version/edition policy, the README workflow
surface, and the changelog release summary aligned with the current release
decision.
`scripts/feature-boundary-scan.sh` verifies that default builds do not pull the
optional macro or serde payload dependencies, that `macros` pulls only the
macro crate, and that `serde` pulls the JSON serialization helpers without
pulling the proc-macro crate. It also checks the `macros`-only and `serde`-only
feature combinations compile.
`cargo package --list --allow-dirty` is part of the release gate as a
package file-list smoke. The package surface keeps local CI workflows, agent
instructions, and release tooling scripts out of the crate artifact. Full `cargo package -p ... --allow-dirty` verification is also part of the release gate;
publishing remains the separate irreversible networked release step.
`scripts/release-gate-shape-scan.sh` keeps the release gate itself aligned with
the final checklist. The release gate runs scan self-tests first so the patterns
prove they catch synthetic leaks before checking the real tree. The release gate
also builds default and all-feature rustdoc with warnings denied through
`RUSTDOCFLAGS="-D warnings" cargo doc --no-deps`.
It runs `cargo clippy --all-targets -- -D warnings` without a release
allow-list. Macro work is checked through feature-disabled, all-feature, macro
crate, all-feature example, and all-feature rustdoc commands.

For release candidates, run the full gate twice in a row:

```sh
scripts/final-release-gate.sh
```

## Codex Workflow

The project is structured around a codex workflow, where the `AGENTS.md` file
defines the project goals, reference policies, and TODO policies for contributors,
while the `CHANGELOG.md` file records meaningful project changes over time.
