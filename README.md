# Asynqrs

Rust learning/reimplementation project for
[hibiken/asynq](https://github.com/hibiken/asynq), a Go Redis-backed task queue.

The crate preserves Asynq-compatible workflow and Redis wire behavior where
that matters, while using Rust-native APIs and ownership internally. The current
server runtime is server-owned: users build Redis-backed clients, servers,
schedulers, and inspectors through typed constructors rather than constructing a
`Processor` runtime object.

This project is prepared for publication. `Cargo.toml` now keeps
`publish = true` after an explicit release decision. Strict Redis smoke evidence
and the final two-pass release gate are recorded locally with Docker-backed
testcontainers.

## Public Workflows

- Enqueue tasks with `RedisBackedClient`, `Task`, and `EnqueueOptions`.
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

## Verification

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
`cargo package --list --allow-dirty` is part of the release gate as a
network-free package file-list smoke. The package surface keeps local CI
workflows, agent instructions, and release tooling scripts out of the crate
artifact. Full package verification and publishing remain separate networked release steps,
not part of the offline release gate.
`scripts/release-gate-shape-scan.sh` keeps the release gate itself aligned with
the final checklist. The release gate runs scan self-tests first so the patterns
prove they catch synthetic leaks before checking the real tree. The release gate
also builds rustdoc with warnings denied through
`RUSTDOCFLAGS="-D warnings" cargo doc --no-deps`.
It runs `cargo clippy --all-targets -- -D warnings` without a release
allow-list.

Before publishing, run the full gate twice in a row:

```sh
scripts/final-release-gate.sh
```

## Codex Workflow

The project is structured around a codex workflow, where the `AGENTS.md` file
defines the project goals, reference policies, and TODO policies for contributors,
while the `CHANGELOG.md` file records meaningful project changes over time.
