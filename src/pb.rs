//! Protobuf message modules generated from Asynq compatibility schemas.
//!
//! Reference: Asynq v0.26.0 internal protobuf schema:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/internal/proto/asynq.proto>.
//!
//! Keep this handwritten module entry outside `src/pb/`; `buf generate --clean`
//! owns and recreates that generated output directory.

pub mod asynq;
