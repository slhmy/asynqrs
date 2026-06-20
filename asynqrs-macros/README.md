# asynqrs-macros

Proc macros for [`asynqrs`](https://crates.io/crates/asynqrs) typed task
ergonomics.

This crate is a small companion crate for `asynqrs`. Most users should enable
the macros through the main crate instead of depending on this crate directly:

```toml
[dependencies]
asynqrs = { version = "0.2", features = ["macros", "serde"] }
serde = { version = "1", features = ["derive"] }
```

The first macro is `#[derive(TaskPayload)]`, which implements
`asynqrs::TypedTaskPayload` for a serializable Rust payload type:

```rust
use asynqrs::{TaskPayload, TypedTaskPayload};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, TaskPayload)]
#[task_type = "email:welcome"]
struct WelcomeEmail {
    user_id: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let task = WelcomeEmail { user_id: 42 }.into_task()?;
    assert_eq!(task.task_type(), WelcomeEmail::TASK_TYPE);
    Ok(())
}
```

The generated implementation calls public `asynqrs` APIs only, including the
main crate's serde-gated JSON helpers. It does not change Redis wire behavior,
queue routing, handler execution, or the explicit non-macro task API.

## Macro API

This crate currently exposes one derive macro:

- `TaskPayload`: reads `#[task_type = "..."]`, implements
  `asynqrs::TypedTaskPayload`, and uses the main crate's JSON helpers for
  payload encoding and decoding.

The derive validates task type metadata at compile time where possible. Missing,
blank, duplicate, and non-string `task_type` attributes produce compile errors
instead of generating partial runtime glue.

## Feature Boundary

- `asynqrs/macros` enables macro re-exports from the main crate.
- `asynqrs/serde` enables JSON payload encode/decode helpers used by the derive.
- `#[derive(TaskPayload)]` requires both `asynqrs/macros` and `asynqrs/serde`.
- Default `asynqrs` builds do not depend on this proc-macro crate.

## Publishing

For a release, publish this crate before publishing the main `asynqrs` crate.
The main crate's optional macro dependency resolves through the crates.io index
during package verification.
