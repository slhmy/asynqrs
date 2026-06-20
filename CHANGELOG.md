# CHANGELOG

This file is compressed project memory for the Asynq v0.26.0 alignment work.
Git history remains the source for exact per-commit detail. Keep this document
release-facing: current state, active blockers, meaningful behavior/API changes,
and concise upstream references only.

## Current Release Summary

This project is staged for final `asynqrs v0.2.0` publication. `Cargo.toml`
keeps `publish = true` after an explicit release decision. `asynqrs-macros
v0.2.0` is published to crates.io, and `cargo package -p asynqrs --allow-dirty`
now passes with the optional macro dependency resolved from the crates.io index.
Strict Redis smoke evidence passes locally with Docker-backed testcontainers;
the final two-pass release gate must be rerun before publishing `asynqrs
v0.2.0`.

Current release-facing state:

- Runtime ownership is server-native. The transitional `Processor` shell has
  been removed; `Server`, `ServerRuntimeState`, `WorkerAssembly`, `Worker`, and
  `TaskExecutor` own the runtime path.
- Public workflows are crate-root and Redis-builder oriented: clients,
  servers, schedulers, inspectors, handlers, middleware, aggregation hooks,
  typed config, and task models are the intended user surface.
- Implementation-shaped runtime state and internal adapter types are kept out
  of the public API. Remaining crate-private runtime traits are documented in
  `docs/rust-native-runtime-redesign.md`.
- Release-facing docs are intentionally small: public API, migration,
  alignment gaps, runtime redesign, Redis smoke matrix, release roadmap, and
  this changelog.

Known release blockers:

- Rerun the final two-pass release gate in a strict Redis smoke-capable
  environment, then publish `asynqrs v0.2.0`.

## 2026-06-20

### 0.2.0 Macro Ergonomics Foundation

- Added an optional `asynqrs-macros` proc-macro crate and feature gates for
  `macros` and `serde` so macro ergonomics do not affect default builds.
- Added `TypedTaskPayload`, `TaskPayloadError`, serde-gated JSON payload helpers,
  and `#[derive(TaskPayload)]` for typed payloads with explicit task type
  metadata.
- Added `TypedHandlerFunc`, `typed_handler`, `ServeMux::handle_typed`,
  `ServeMux::route_typed`, and `serve_mux!` for typed payload handler
  registration with explicit decode-error mapping to `HandlerError::Failed`.
- Added `examples/typed_payload.rs`, `examples/macro_handlers.rs`, and release
  metadata guards for the new workspace/package artifacts.
- Added `asynqrs-macros` README plus crate-level and derive-level rustdoc so the
  companion proc-macro package explains feature usage, validation behavior, and
  staged publishing on crates.io.
- Routed derive macro serde bounds through the main crate's serde-gated public
  path so generated code depends on `asynqrs` APIs rather than hidden runtime
  internals or direct macro-crate assumptions.
- Added `trybuild` compile-fail coverage for missing, blank, and duplicate
  `task_type` derive attributes, non-string `task_type` metadata, plus the
  `serve_mux!` non-typed-payload error path; release metadata scanning now
  requires those UI fixtures in the package file list.
- Added direct serde helper tests for `TaskPayloadError::Encode` and
  `TaskPayloadError::Decode` mapping.
- Clarified typed payload rustdoc so the public trait, error variants, JSON
  helpers, and macro crate docs describe feature boundaries without exposing
  runtime internals.
- Clarified release-facing docs that manual `TypedTaskPayload` implementations
  are available without macros/serde, while `#[derive(TaskPayload)]` requires
  both `macros` and `serde` for the JSON-backed implementation.
- Updated the README for the 0.2.0 published user surface, including install
  snippets for default and macro-enabled builds plus typed payload behavior
  notes.
- Added `scripts/feature-boundary-scan.sh` to prove default builds do not pull
  the optional macro or serde dependencies, check `macros`-only and
  `serde`-only feature compilation, verify the macros-only derive failure path,
  and wired it into the release gate shape checks.
- Added all-feature rustdoc with warnings denied to the release gate so
  macro-gated public APIs are documented under the same strict warning policy as
  default docs.
- Published `asynqrs-macros v0.2.0` to crates.io. Package verification evidence:
  `cargo package -p asynqrs-macros --allow-dirty` and
  `cargo package -p asynqrs --allow-dirty` pass for 0.2.0 after the optional
  macro crate dependency resolves through the crates.io index.
- Reference: Asynq v0.26.0 task construction still maps to ordinary task type
  and payload bytes:
  <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L22-L73>.

### Publication Decision

- Renamed the crate package from `asynq-rs` to `asynqrs` to avoid the existing
  crates.io package name and give Rust users a concise `asynqrs::...` import
  path.
- Anchored package excludes to root-level local tooling paths so Redis script
  source modules under `src/broker/redis/scripts` remain in the published
  package.
- Enabled crate publication by changing `Cargo.toml` to `publish = true` after
  the explicit release decision.
- Updated release-facing docs and metadata scans so the package metadata,
  release summary, and roadmap agree that no active release blocker remains.
- TODO: Re-run `scripts/final-release-gate.sh` immediately before the actual
  networked publish step.
- Reference: Asynq v0.26.0 compatibility target remains
  <https://github.com/hibiken/asynq/tree/v0.26.0>.

### Rust-Native Runtime Finalization

- Removed the transitional `Processor` shell and moved server runtime ownership
  to server-owned components: worker assembly, active-worker tracking,
  cancellation, pending sync, shutdown/requeue, maintenance, metadata heartbeat,
  lease extension, and task execution now live under server/runtime or
  processing ownership.
- Renamed processor-shaped runtime concepts to worker/runtime language and
  tightened semantic scans so legacy `crate::processor` paths,
  processor-shaped local runtime names, `with_server_runtime`, silent no-op
  server trait defaults, and stale processor ownership wording fail before
  release.
- Changed `ServerRuntimeStateAttach` to mutable
  `attach_server_runtime(&mut self, ...)`, making server-owned runtime state
  explicit before a runtime owner is moved into `Server`.
- Removed `ServerRuntimeState` test-only constructors and counters from the
  production type; server tests now create pending-sync and active-worker
  fixtures through private `server::test_support` helpers while the runtime
  state keeps only lifecycle behavior.
- Moved parallel server runtime test entry points off the production `Server`
  impl and into private `server::test_support` helpers backed by crate-private
  lifecycle primitives; also removed conditional compilation from
  crate-private maintenance summary accessors.
- Removed the test-only `ServerCanceller` active-task registry snapshot helper;
  cancellation tests now assert observable cancellation behavior after
  unregister instead of inspecting internal task-id maps.
- Moved the single-worker server run test wrapper out of the production
  `Server` impl and into private test helpers, and removed the cfg-only
  `ServerState::Stopped` simulation that was not produced by the current
  Rust-native lifecycle.
- Removed cfg-only server builder hooks for logger injection, health-check
  handler injection, and aggregation group-config inspection; tests now use
  private `server::test_support` and the server test extension trait.
- Moved the pending-sync `WorkerAssembly::run_sync_once` test helper out of the
  production pending-sync backlog module and into worker assembly test helpers.
- Moved the metadata timestamp normalization helper out of production codec
  code and into metadata encoding tests.
- Removed cfg-only `ResultWriter::channel` and `RedisCancelListener::canceller`
  test helpers; tests now construct writer channels locally or assert against
  the caller-owned canceller clone.
- Moved the Redis protobuf timestamp normalization test helper out of parse
  production code and into the parse tests.
- Documented retained crate-private runtime traits in
  `docs/rust-native-runtime-redesign.md`; the release metadata scan derives the
  trait list from current server runtime source and fails if a retained boundary
  is undocumented.
- Reference: Asynq v0.26.0 server, processor, heartbeat, syncer, recoverer,
  janitor, and subscriber lifecycle:
  <https://github.com/hibiken/asynq/tree/v0.26.0>.

### Public API Freeze

- Kept `src/lib.rs` as a workflow facade. Crate-root exports now lead with
  Redis-backed client/server/scheduler/periodic/Inspector builders, typed
  task/config models, handler routing, middleware hooks, aggregation hooks, and
  structured errors.
- Added common workflow exports to the prelude, including `ServeMux`,
  `TaskMiddlewareHooks`, `ProcessingScope`, `ServerProcessingScope`,
  `TaskMetadata`, periodic manager types, aggregation hooks, and primary
  Inspector read models.
- Narrowed admin metadata codec helpers to crate-internal use. Public
  applications read server, worker, and scheduler metadata through typed
  `Inspector` methods instead of direct Redis metadata codec helpers.
- Kept `scheduler::SchedulerBroker` and `client::{Broker, AsyncBroker,
  CloseBroker}` as module-level advanced extension points while keeping broker
  internals out of the crate-root workflow facade.
- Tightened `scripts/public-api-scan.sh` so it verifies real crate-root and
  prelude export blocks, rejects public re-exports of runtime/state/internal
  broker traits, rejects public admin metadata codec helper leaks, keeps
  `processing` private, keeps `Config` fields private, and prevents admin
  cancellation, Inspector broker submodules, and aggregation broker internals
  from becoming public module API.
- Extended the public API scan to reject test-only `pub use`/`pub mod`
  facades behind `#[cfg(test)]`, keeping tests pointed at real module
  boundaries instead of hidden public re-export paths.
- Added and linked compiled workflow examples for enqueue, server processing,
  middleware hooks, handler failure, graceful shutdown, scheduler registration,
  Inspector metadata reads, and aggregation customization.
- Reference: Asynq v0.26.0 public quick-start and Inspector/Scheduler APIs:
  <https://github.com/hibiken/asynq/blob/v0.26.0/README.md> and
  <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go>.

### Redis and Release Gates

- Added strict Redis smoke preflight and release-gate shape scans. The local
  release gate now defaults `ASYNQ_RS_REDIS_STRICT=1`, runs scan self-tests
  before real scans, runs a network-free package file-list smoke, runs Redis
  preflight before strict smoke, compiles examples, runs doctests/full tests,
  and finishes with `git diff --check`.
- Added strict rustdoc generation to the release gate with
  `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps`.
- Added clippy to the release gate with plain
  `cargo clippy --all-targets -- -D warnings` and cleaned the internal
  async-worker and assembly-helper lints instead of carrying a release
  allow-list.
- Tightened release metadata scanning so the package file list must include the
  release docs/examples and must not contain deleted `src/processor` module
  files.
- Updated Redis smoke evidence docs and metadata scans so the package file-list
  smoke remains part of the recorded pre-Redis release-gate evidence.
- Excluded local CI workflows, agent instructions, and release tooling scripts
  from the crate package, and tightened metadata scans so these local assets do
  not leak back into `cargo package --list --allow-dirty`.
- Added `scripts/final-release-gate.sh` as the two-pass release wrapper and
  guarded CI so it runs the same release gate with strict Redis mode, a Redis
  URL, and a Redis service.
- Documented that GitHub CI should use the Redis service-container path through
  `ASYNQ_RS_REDIS_URL` rather than relying on Docker-in-Docker; Docker-backed
  `testcontainers` remains the local fallback when no Redis URL is supplied.
- Installed `ripgrep` explicitly in CI before running release scan scripts,
  because the release gate relies on `rg` and GitHub-hosted images do not
  guarantee it is present.
- Tightened the release-gate shape scan so the final two-pass wrapper must keep
  explicit pass progress output, making CI/local logs show which pass ran.
- Tightened the release-gate shape scan so package file-list smoke must run
  before Redis preflight, Redis preflight must run before strict Redis smoke,
  and strict Redis smoke must run before examples.
- Clarified that `scripts/redis-smoke-preflight.sh` proves only that a Redis
  target or Docker-backed testcontainers path exists; strict Redis smoke still
  proves reachability and behavior.
- Tightened Redis smoke preflight diagnostics so missing local `redis-server`
  and `redis-cli` binaries are reported alongside missing Redis URL or Docker
  daemon evidence.
- Isolated Redis smoke preflight self-tests from the runner PATH so CI images
  that already include Docker or Redis CLIs cannot make the missing
  infrastructure test pass accidentally.
- Removed raw enqueue-option helper methods from the production `EnqueueOptions`
  impl; normal callers and tests now use typed `QueueName`, `TaskId`, and
  `GroupName` setters, while raw-string validation coverage constructs
  crate-private fields directly.
- Removed a stale Redis runtime re-export `unused_imports` allowance; clippy now
  verifies the re-export set without a local suppression.
- Removed stale Redis key-helper dead-code/import allowances and clarified that
  the release gate uses the offline package file-list smoke; full package
  verification or publishing remains a separate networked release step.
- Removed a self-equality Redis key-helper test; concrete key string tests now
  carry the Redis namespace coverage.
- Replaced self-equality release tests with concrete expected values for Redis
  plan constants, public error messages, the target Asynq version, and scheduler
  entry accessors.
- Removed stale test lint allowances by narrowing server worker-runtime fixture
  imports and initializing Redis metadata fake executors with struct literals
  instead of post-`Default` field reassignment.
- Removed the Redis plan module-wide `dead_code` allowance by deleting stale
  inspection-only plan fields, accessors, and alternate constructors; plan
  tests now assert executable Redis keys/arguments instead of mirrored inputs.
- Removed low-risk test-only accessors from client, scheduler, and worker
  assembly tests by reading crate-visible fields directly, and moved
  task-message encode/decode nil-branch helpers into the local test module.
- Removed Inspector test-only constructors/accessors and Aggregator test-only
  configuration/accessor/run helpers from production impls; tests now use local
  test support helpers or crate-visible state inspection instead.
- Removed Server test-only configuration accessors and builder-only test
  setters from production impls; server tests now use local test extensions,
  while cross-module Redis metadata tests no longer depend on server-private
  metadata injection.
- Moved Server test-only constructors out of the production constructor impl;
  server tests now use explicit `server::test_support` constructors, while
  Redis integration tests use the real `Server::with_config` path.
- Removed Redis-backed server builder test-only helper methods; tests now read
  crate-visible builder state directly and keep optional-handler compatibility
  checks in local test helpers.
- Removed Scheduler test-only configuration accessors; scheduler and periodic
  tests now inspect crate-visible scheduler state directly where needed.
- Collapsed redundant `#[cfg(test)]` markers inside the worker assembly
  test-support module so its test-only boundary is the module itself rather
  than repeated per-method gates.
- Tightened the semantic gap scan so deleted low-risk test-only accessors,
  Scheduler and Server test hooks, Aggregator test hooks, Inspector test hooks,
  aggregation model test accessors, and the Redis plan module-wide `dead_code`
  allowance cannot drift back in.
- Tightened Redis test fixtures so configured URL open/connect failures use the
  strict diagnostic path instead of bare `unwrap()` panics.
- Hardened Redis testcontainer fixture startup so transient Docker port
  resolution failures are retried before strict smoke fails.
- Pre-0.2.0 macro local evidence: release scans, package file-list smoke, Redis
  preflight self-tests, clippy with warnings denied, examples,
  doctests, strict rustdoc, full non-Redis tests, formatting, and diff checks
  pass locally. Strict Redis smoke passes with Docker-backed testcontainers
  (`25 passed; 0 failed`), and `scripts/final-release-gate.sh` passes both
  release-gate passes locally before the staged macro crate package split.
- Reference: Asynq v0.26.0 Redis-backed lifecycle behavior and internal Redis
  broker operations:
  <https://github.com/hibiken/asynq/tree/v0.26.0/internal/rdb>.

### Documentation Cleanup

- Compressed release planning into `docs/release-readiness-roadmap.md`, which
  now contains the current release decision, retained documentation set, final
  evidence checklist, and current audit instead of phase-by-phase refactor
  history.
- Kept the docs directory limited to release-facing markdown files:
  `alignment-gaps.md`, `migration.md`, `public-api.md`,
  `redis-smoke-matrix.md`, `release-readiness-roadmap.md`, and
  `rust-native-runtime-redesign.md`.
- Updated README, crate docs, migration guide, public API audit, Redis smoke
  matrix, and runtime redesign docs to describe the current Rust-native
  architecture rather than the deleted Processor transition.
- Tightened `scripts/docs-set-scan.sh`,
  `scripts/release-metadata-scan.sh`, and `scripts/semantic-gap-scan.sh` so
  docs cannot drift back to stale phase plans, scattered source TODO/FIXME
  markers, stale known-gap markers, public metadata codec workflows, missing
  compiled example links, or overclaimed Redis preflight evidence.
- Added release metadata self-test coverage proving stale changelog gap markers
  are rejected before the real changelog is scanned.
- This changelog was compressed from historical refactor notes into
  release-facing project memory; obsolete intermediate Processor-era gaps are
  intentionally left to git history.

## 2026-06-19

### Runtime Ownership Migration

- Moved server lifecycle behavior out of the old Processor path in stages:
  worker construction, maintenance, shutdown/requeue, metadata heartbeat,
  lease extension, pending sync, cancellation state, and worker task execution
  were all migrated toward server-owned runtime components before the final
  Processor shell deletion.
- Promoted handler, middleware, retry, lease, and processing error APIs toward
  the crate-root workflow facade while keeping the internal processing runtime
  private.
- Reference: Asynq v0.26.0 server worker lifecycle and processor task loop:
  <https://github.com/hibiken/asynq/blob/v0.26.0/server.go> and
  <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go>.

## 2026-06-17

### Rust-First Public API

- Added the early Rust-first API audit, package metadata cleanup, README
  workflow orientation, and release documentation set that later became the
  current public API and release-readiness docs.
- Declared the MIT license in `Cargo.toml` and kept `publish = false` until
  release evidence is complete.
- Reference: Asynq v0.26.0 public package concepts:
  <https://github.com/hibiken/asynq/tree/v0.26.0>.
