//! Redis key helpers used by Asynq.
//!
//! Reference: Asynq v0.26.0 `internal/base` Redis key helpers:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L81-L139>.

mod aggregation;
mod metadata;
mod queue;
mod stats;
mod unique;

pub use aggregation::{
    aggregation_set_key, all_aggregation_sets_key, all_groups_key, group_key, group_key_prefix,
};
pub use metadata::{scheduler_entries_key, scheduler_history_key, server_info_key, workers_key};
pub use queue::{
    active_key, archived_key, completed_key, lease_key, paused_key, pending_key, queue_key_prefix,
    retry_key, scheduled_key, task_key, task_key_prefix,
};
pub use stats::{failed_key, failed_total_key, processed_key, processed_total_key};
pub use unique::unique_key;

pub const ALL_SERVERS: &str = "asynq:servers";
pub const ALL_WORKERS: &str = "asynq:workers";
pub const ALL_SCHEDULERS: &str = "asynq:schedulers";
pub const ALL_QUEUES: &str = "asynq:queues";
/// Reference: Asynq v0.26.0 public cancellation pub/sub channel constant:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L37-L40>.
pub const CANCEL_CHANNEL: &str = "asynq:cancel";
#[cfg(test)]
mod tests;
