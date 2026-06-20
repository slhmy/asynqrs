# Release Readiness Goal

This document records the current release decision and the evidence still
needed before `asynqrs` is published. Completed cleanup history belongs in
`CHANGELOG.md`; this file keeps only release-facing checks that help decide
whether the crate remains ready.

## Current Decision

The publication decision has been made. `Cargo.toml` now keeps `publish = true`.
Strict Redis smoke evidence passes locally with Docker-backed testcontainers.
The 0.2.0 release is staged: `asynqrs-macros v0.2.0` is published to crates.io,
and `asynqrs v0.2.0` package verification now resolves the optional macro crate
from the crates.io index. Before publishing the main crate, rerun the final
two-pass release gate in a strict Redis smoke-capable environment.

Release readiness means all of the following are true in the same candidate:

- Public workflows are stable enough for clients, servers, schedulers,
  inspectors, handlers, middleware, and aggregation hooks without depending on
  internal runtime modules.
- Optional macro ergonomics remain an additive layer over public task and
  handler APIs; default builds must not pull in proc-macro or serialization
  dependencies.
- Redis wire behavior and core task lifecycle behavior remain compatible with
  Asynq v0.26.0 where documented.
- Rust-native ownership is the baseline: server-owned runtime state, explicit
  worker execution, typed config, builder validation, and narrow extension
  traits.
- Migration differences from Go Asynq are documented as decisions, not hidden
  accidental gaps.
- Examples, strict Redis smoke coverage, and the full test suite are green.
- Full release-gate completion for 0.2.0 waits on the final two-pass release
  gate rerun described above.

## Release Documentation Set

The docs directory should contain only release-facing markdown:

- `public-api.md`: preferred user workflows and intentional public extension
  points.
- `migration.md`: Rust-first mapping for Asynq users.
- `alignment-gaps.md`: resolved or deferred Asynq compatibility decisions.
- `rust-native-runtime-redesign.md`: current server-owned runtime architecture.
- `redis-smoke-matrix.md`: Redis lifecycle scenarios and current test evidence.
- `release-readiness-roadmap.md`: this release decision, checklist, and audit.

Do not add completed phase plans, review transcripts, or temporary refactor
notes back under `docs/`; put historical detail in `CHANGELOG.md` or git
history instead.

## Final Evidence Checklist

Before publishing, collect direct evidence for all of the following after the
final code change:

- `scripts/final-release-gate.sh` passes with `ASYNQ_RS_REDIS_STRICT=1`.
- The Redis smoke pass is non-skipped and uses either `ASYNQ_RS_REDIS_URL`
  against a real Redis instance or a working Docker daemon for `testcontainers`.
- `scripts/redis-smoke-preflight.sh --self-test` passes, and
  `scripts/redis-smoke-preflight.sh` passes in the release environment before
  the strict Redis smoke command runs.
- `docs/redis-smoke-matrix.md` records the passing strict Redis evidence.
- `scripts/public-api-scan.sh --self-test` and `scripts/public-api-scan.sh`
  pass.
- `scripts/semantic-gap-scan.sh --self-test` and
  `scripts/semantic-gap-scan.sh` pass.
- `scripts/docs-set-scan.sh --self-test` and `scripts/docs-set-scan.sh` confirm
  no stale planning docs or temporary review notes were added back under
  `docs/`.
- `scripts/release-metadata-scan.sh --self-test` and
  `scripts/release-metadata-scan.sh` confirm Rust version/edition policy,
  Cargo package metadata, README workflow names, crate-level workflow docs, and
  the changelog release summary/blockers still match the current release
  decision.
- `scripts/feature-boundary-scan.sh --self-test` and
  `scripts/feature-boundary-scan.sh` confirm default builds do not pull
  `asynqrs-macros`, `serde`, or `serde_json`; `macros` pulls the macro crate
  without serialization dependencies; and `serde` pulls the serialization
  helpers without the macro crate. It also checks that `macros`-only and
  `serde`-only feature combinations compile.
- `cargo package --list --allow-dirty` confirms Cargo can produce the package
  file list for the current release candidate.
  `scripts/release-metadata-scan.sh` also verifies that package list includes
  release docs/examples, no deleted `src/processor` module files, and no local
  CI workflows, agent instructions, or release tooling scripts. Full
  `cargo package -p ... --allow-dirty` verification is part of the release gate;
  publishing remains the separate irreversible networked release step.
- `scripts/release-gate-shape-scan.sh --self-test` and
  `scripts/release-gate-shape-scan.sh` confirm the release gate still contains
  strict Redis smoke, package-list smoke, examples, doctests, default and
  all-feature rustdoc with warnings denied, clippy with warnings denied, macro crate tests,
  feature-disabled tests, all-feature tests, the feature boundary scan,
  all-feature examples, full tests, buf, formatting, scans, and whitespace
  checks, that the final gate still runs two passes, and that CI still runs the
  same release gate with strict Redis mode, a Redis URL, and a Redis service
  rather than relying on Docker-in-Docker.
- `CHANGELOG.md` top summary states that `asynqrs-macros v0.2.0` is published,
  main package verification passes, and the remaining release blocker is the
  final two-pass gate plus `asynqrs v0.2.0` publish.
- For 0.2.0 macro ergonomics, publish order is explicit: run package
  verification for `asynqrs-macros`, publish `asynqrs-macros`, then run package
  verification for `asynqrs` and publish `asynqrs`.
- Current package evidence: `cargo publish -p asynqrs-macros --allow-dirty`
  published `asynqrs-macros v0.2.0`, and
  `cargo package -p asynqrs --allow-dirty` now passes for 0.2.0 with the macro
  crate resolved through the crates.io index.

## Current Release Audit

This audit tracks current evidence for the goal. A proven local item does not
publish the crate; the actual `cargo publish` remains a separate networked
release step.

| Requirement | Current Status | Evidence | Remaining Work |
| --- | --- | --- | --- |
| No temporary `Processor` shell owns core server lifecycle behavior. | Proven locally | The remaining handler/execution/retry/lease customization code lives under crate-private `processing`. Runtime architecture docs state `Server` owns runtime state, `WorkerAssembly` owns concrete runtime parts, and `Worker` owns one-task execution. Public `Processor` construction surfaces are absent outside historical `CHANGELOG.md` entries. | Keep release scans guarding against a processor-shaped runtime owner or legacy processor module path. |
| Maintenance, shutdown, requeue, metadata, cancellation, active-worker, and pending-sync ownership lives outside `Processor`. | Proven locally | Server-owned modules cover active workers, cancellation, pending sync, shutdown, maintenance brokers/runners, metadata heartbeat, worker runtime, and worker assembly. The runtime redesign doc records these as current ownership boundaries. | None known for ownership; future cleanup can still fold single-implementation internal traits when a stronger concrete boundary exists. |
| Remaining internal adapter traits are deleted or documented as real boundaries. | Proven locally | Test-only dequeue, complete, result, and server-only stale aggregation reclaim aliases are deleted. `docs/rust-native-runtime-redesign.md` lists retained internal traits and why each remains. Server broker capability traits are crate-private runtime/test boundaries. | Re-audit retained capability traits when new runtime traits are added or when a boundary becomes a single-implementation pass-through. |
| Public API feels Rust-native and workflow-oriented. | Proven locally | `docs/public-api.md`, README, and crate docs lead with Redis-backed builders, typed config, handlers, middleware, scheduler, Inspector, and aggregation workflows. Implementation-shaped runtime state was removed from the public surface. `scripts/public-api-scan.sh --self-test` and the real scan guard the current facade. | Re-run public API scans before publishing or after adding public names. |
| Optional macro ergonomics stay additive. | Macro crate published; main package verified | `TypedTaskPayload`, `TaskPayloadError`, serde-gated JSON helpers, `#[derive(TaskPayload)]`, typed handler adapters, and `serve_mux!` are feature-gated or additive. The release gate shape requires macro crate tests, `--no-default-features`, `--all-features`, the feature boundary scan, all-feature examples, and all-feature rustdoc. `trybuild` covers missing, blank, and duplicate `task_type` derive attributes plus the `serve_mux!` non-payload error path. `asynqrs-macros` has README/crate docs and is published as `v0.2.0`; `cargo package -p asynqrs --allow-dirty` now passes. | Rerun the final two-pass gate, then publish `asynqrs v0.2.0`. |
| Go Asynq differences are explicit. | Proven locally | Migration, public API, alignment gaps, and source `Reference:` comments explain Rust-native choices around Redis construction, config builders, handler context, scheduler registration, metadata codecs, and runtime ownership. `scripts/semantic-gap-scan.sh --self-test` and the real scan guard stale gap markers and processor-shaped runtime wording. | Re-run semantic scans before publishing or after adding public APIs, especially after Redis smoke evidence is recorded. |
| Docs describe current architecture, not refactor history. | Proven locally | Docs directory is limited to the release documentation set above. `scripts/docs-set-scan.sh --self-test` and the real scan guard extra or missing docs. `CHANGELOG.md` is compressed release-facing project memory with the current API/architecture state and known blockers; obsolete intermediate Processor-era gaps are left to git history. | Keep future changelog additions grouped and concise so the release summary stays usable. |
| Key migration examples are executable. | Proven locally | README and migration guide point key workflows to compiled examples: enqueue, server processing, middleware hooks, handler failure, graceful shutdown, scheduler registration, inspector metadata reads, and aggregation customization. CI and local verification run `cargo test --examples`; crate-level docs contain compile-checked `no_run` snippets. | Convert additional prose snippets into doctests only if doc drift becomes a recurring problem. |
| Redis smoke coverage exercises core lifecycle paths. | Proven locally | `docs/redis-smoke-matrix.md` records current Redis-backed scenarios and the strict serial command for `broker::redis::tests::*`. GitHub CI runs that strict command against a Redis service container through `ASYNQ_RS_REDIS_URL`, not Docker-in-Docker. Local strict Redis smoke now passes with Docker-backed testcontainers: `25 passed; 0 failed; 0 ignored`. | Re-run strict Redis smoke before publishing or after Redis/runtime changes. |
| Full verification is green twice in a row. | Needs final gate rerun before main publish | `scripts/final-release-gate.sh` is the two-pass gate. Each pass includes buf, formatting, clippy with warnings denied, release-gate shape, metadata, package file-list smoke, full package verification, docs-set, public API, semantic-gap, Redis preflight, strict Redis smoke, examples, doctests, default and all-feature strict rustdoc, full tests, and `git diff --check`. | Rerun the final gate in a strict Redis smoke-capable environment, then publish `asynqrs v0.2.0`. |
