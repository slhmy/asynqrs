//! Redis-backed operation implementations for `RedisBroker`.
//!
//! The broker root owns structure (`model`, `error`, `adapters`, parsing, and
//! script result helpers). This module owns the inherent Redis operation methods
//! grouped by Asynq domain.

use std::time::{Duration, SystemTime};

mod admin_error;
mod aggregating_task_admin;
mod aggregation;
mod core;
mod lease;
mod lifecycle;
mod maintenance;
mod metadata;
mod queue_admin;
mod queue_stats;
mod result_cancel;
mod task_admin;
mod task_collection_admin;

pub(in crate::broker::redis::broker) use super::parse;
pub(in crate::broker::redis::broker) use super::{
    RedisBroker, RedisBrokerError, map_script_result, redis_broker_error_matches_not_found,
    redis_broker_error_message,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::broker::redis::broker) struct AggregationCheckRequest<'a> {
    pub(in crate::broker::redis::broker) queue: &'a str,
    pub(in crate::broker::redis::broker) group: &'a str,
    pub(in crate::broker::redis::broker) set_id: String,
    pub(in crate::broker::redis::broker) now: SystemTime,
    pub(in crate::broker::redis::broker) grace_period: Duration,
    pub(in crate::broker::redis::broker) max_delay: Duration,
    pub(in crate::broker::redis::broker) max_size: isize,
}
