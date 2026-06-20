# AGENTS.md

## Project Goal

This repository is a Rust learning/reimplementation project for
[hibiken/asynq](https://github.com/hibiken/asynq), a Go Redis-backed task queue.

Prefer matching upstream concepts and behavior first, but do not copy Go's
package structure, nil/variadic idioms, or runtime ownership model when Rust has
a clearer native shape. Rust-native API and architecture are first-class project
goals: use types, ownership, builders, enums, traits, and async boundaries to
make invalid states harder to express and runtime behavior easier to reason
about. Semantic differences from upstream should be explicit.

**Any new alignment task will not be undertaken if it fails to satisfy at least one of the following criteria:**

- Completing a user-facing core API
- Fixing an issue that leads to erroneous results or inconsistent states
- Significantly reducing migration costs for Asynq users
- Significantly enhancing test reliability or maintainability
- Paving the way for Rust-native capabilities

## Reference Policy

When adding a module, type, function, proto schema, or behavior based on Asynq,
include a short `Reference:` comment or doc comment near the implementation.

Use fixed upstream tags instead of floating branches. Current baseline:

```text
https://github.com/hibiken/asynq/tree/v0.26.0
```

Good examples:

```rust
/// Reference: Asynq v0.26.0 public `TaskState` constants:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L207-L250>.
```

```proto
// Source: https://github.com/hibiken/asynq/blob/v0.26.0/internal/proto/asynq.proto#L1-L71
```

## TODO Policy

If an upstream field, option, behavior, or lifecycle hook is intentionally not
implemented yet, leave a `TODO:` comment close to the incomplete area.

The TODO should say what is missing and when it should be added. For example:

```rust
// TODO: Add task options once enqueue behavior is modeled.
// Upstream stores `opts []Option` on Task and applies them when enqueuing.
```

Avoid silent omissions when copying upstream structures.

## Best Practices

Keep code organized into focused modules with clear ownership boundaries.
Avoid overly long single files, catch-all modules, or broad implementations that
mix unrelated concepts. When a file starts accumulating multiple responsibilities,
split it by domain, lifecycle stage, protocol boundary, or upstream Asynq concept
before adding more behavior.

Prefer small, coherent functions and types that make the task queue semantics easy
to inspect. Do not hide important behavior in large helper blocks, deeply nested
control flow, or generic abstractions that are broader than the current feature
needs.

## Change Log Policy

Record each meaningful project change in `CHANGELOG.md`.

Each entry should include:

- date
- short summary of what changed
- upstream reference if the change mirrors Asynq behavior
- TODOs or known gaps introduced by the change

Keep entries concise. The log is for project memory across sessions, not a full
commit diff.

## Verification

After code or schema changes, run the relevant checks:

```sh
buf lint
cargo fmt --check
cargo test
```
