//! Redis command-plan errors grouped by upstream operation family.
//!
//! Reference: Asynq v0.26.0 Redis RDB methods group enqueue, dequeue,
//! lifecycle, aggregation, metadata, and inspector/admin command behavior in
//! separate operation families:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.

// Some validation branches are kept as named variants to mirror the upstream
// operation boundary even when the current caller normalizes before reaching
// the Redis plan layer.
#[allow(dead_code)]
mod admin;
#[allow(dead_code)]
mod aggregation;
#[allow(dead_code)]
mod dequeue;
mod enqueue;
#[allow(dead_code)]
mod lifecycle;
#[allow(dead_code)]
mod metadata;

pub use admin::RedisAdminPlanError;
pub use aggregation::{
    RedisAggregationCheckPlanError, RedisDeleteAggregationSetPlanError,
    RedisReadAggregationSetPlanError, RedisReclaimStaleAggregationSetsPlanError,
};
pub use dequeue::RedisDequeuePlanError;
pub use enqueue::RedisEnqueuePlanError;
pub use lifecycle::{
    RedisArchivePlanError, RedisCompletePlanError, RedisDeleteExpiredCompletedTasksPlanError,
    RedisExtendLeasePlanError, RedisForwardPlanError, RedisRecoverPlanError, RedisRequeuePlanError,
    RedisRetryPlanError, RedisWriteResultPlanError,
};
pub use metadata::RedisMetadataPlanError;
