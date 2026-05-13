# AGENTS.md

## Project Goal

This repository is a Rust learning/reimplementation project for
[hibiken/asynq](https://github.com/hibiken/asynq), a Go Redis-backed task queue.

Prefer matching upstream concepts and behavior first. Rust-specific API design is
fine when it makes the code safer or more idiomatic, but semantic differences
from upstream should be explicit.

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

## Docs

Belong with the project moving forward, check if tutorials under `docs/` need to be updated or added to reflect new features or changes.
