//! Aggregation Redis command plans.
//!
//! Reference: Asynq v0.26.0 aggregation-set Redis commands:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1126-L1261>.

use std::time::{Duration, SystemTime};

use crate::broker::redis::keys;
use crate::compat::MAX_DURATION_NANOS_U128;
use crate::task::duration_seconds;

use super::{
    AGGREGATION_TIMEOUT, RedisAggregationCheckPlanError, RedisArg,
    RedisDeleteAggregationSetPlanError, RedisReadAggregationSetPlanError,
    RedisReclaimStaleAggregationSetsPlanError, RedisScript, RedisScriptCall, unix_seconds_floor,
};

/// Redis command intent for moving ready group tasks into an aggregation set.
///
/// Reference: Asynq v0.26.0 `RDB.AggregationCheck`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1126-L1143>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisAggregationCheckPlan {
    call: RedisScriptCall,
    set_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisAggregationCheckInput<'a> {
    pub queue: &'a str,
    pub group: &'a str,
    pub set_id: String,
    pub check_time: SystemTime,
    pub deadline_base: SystemTime,
    pub grace_period: Duration,
    pub max_delay: Duration,
    pub max_size: isize,
}

impl RedisAggregationCheckPlan {
    pub fn from_group(
        input: RedisAggregationCheckInput<'_>,
    ) -> Result<Self, RedisAggregationCheckPlanError> {
        let set_id = input.set_id;
        // Reference: Asynq v0.26.0 `RDB.AggregationCheck` passes the caller's
        // queue, group, and aggregation set id through Redis keys without
        // local validation. It passes the caller's check time as ARGV[5], but
        // computes the aggregation-set expiration from
        // `r.clock.Now().Add(AGGREGATION_TIMEOUT)`.
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1126-L1143>.
        let expire_time = input.deadline_base.checked_add(AGGREGATION_TIMEOUT).ok_or(
            RedisAggregationCheckPlanError::TimeOverflow("aggregation set expiration"),
        )?;

        Ok(Self {
            call: RedisScriptCall::new(
                RedisScript::AggregationCheck,
                vec![
                    keys::group_key(input.queue, input.group),
                    keys::aggregation_set_key(input.queue, input.group, &set_id),
                    keys::all_aggregation_sets_key(input.queue),
                    keys::all_groups_key(input.queue),
                ],
                vec![
                    RedisArg::I64(input.max_size.try_into().map_err(|_| {
                        RedisAggregationCheckPlanError::TimeOverflow("aggregation max size")
                    })?),
                    RedisArg::I64(aggregation_duration_seconds(
                        input.max_delay,
                        "aggregation max delay",
                    )?),
                    RedisArg::I64(aggregation_duration_seconds(
                        input.grace_period,
                        "aggregation grace period",
                    )?),
                    RedisArg::I64(unix_seconds_aggregation(
                        expire_time,
                        "aggregation set expiration",
                    )?),
                    RedisArg::I64(unix_seconds_aggregation(
                        input.check_time,
                        "aggregation check time",
                    )?),
                    RedisArg::String(input.group.to_owned()),
                ],
            ),
            set_id,
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }

    pub fn set_id(&self) -> &str {
        &self.set_id
    }
}

/// Redis command intent for deleting an aggregation set and its task messages.
///
/// Reference: Asynq v0.26.0 `RDB.DeleteAggregationSet`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1228-L1235>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisDeleteAggregationSetPlan {
    call: RedisScriptCall,
}

impl RedisDeleteAggregationSetPlan {
    pub fn from_set(
        queue: &str,
        group: &str,
        set_id: &str,
    ) -> Result<Self, RedisDeleteAggregationSetPlanError> {
        // Reference: Asynq v0.26.0 `RDB.DeleteAggregationSet` derives the
        // aggregation set key directly from the supplied queue, group, and set
        // id:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1228-L1235>.
        Ok(Self {
            call: RedisScriptCall::new(
                RedisScript::DeleteAggregationSet,
                vec![
                    keys::aggregation_set_key(queue, group, set_id),
                    keys::all_aggregation_sets_key(queue),
                ],
                vec![RedisArg::String(keys::task_key_prefix(queue))],
            ),
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}

/// Redis command intent for reading messages in an aggregation set.
///
/// Reference: Asynq v0.26.0 `RDB.ReadAggregationSet`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1177-L1205>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisReadAggregationSetPlan {
    call: RedisScriptCall,
    aggregation_set_key: String,
    all_aggregation_sets_key: String,
}

impl RedisReadAggregationSetPlan {
    pub fn from_set(
        queue: &str,
        group: &str,
        set_id: &str,
    ) -> Result<Self, RedisReadAggregationSetPlanError> {
        // Reference: Asynq v0.26.0 `RDB.ReadAggregationSet` derives the
        // aggregation set key directly from the supplied queue, group, and set
        // id:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1177-L1205>.
        let aggregation_set_key = keys::aggregation_set_key(queue, group, set_id);
        let all_aggregation_sets_key = keys::all_aggregation_sets_key(queue);
        Ok(Self {
            call: RedisScriptCall::new(
                RedisScript::ReadAggregationSet,
                vec![aggregation_set_key.clone()],
                vec![RedisArg::String(keys::task_key_prefix(queue))],
            ),
            aggregation_set_key,
            all_aggregation_sets_key,
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }

    pub fn aggregation_set_key(&self) -> &str {
        &self.aggregation_set_key
    }

    pub fn all_aggregation_sets_key(&self) -> &str {
        &self.all_aggregation_sets_key
    }
}

/// Redis command intent for reclaiming stale aggregation sets.
///
/// Reference: Asynq v0.26.0 `RDB.ReclaimStaleAggregationSets`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1256-L1261>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisReclaimStaleAggregationSetsPlan {
    call: RedisScriptCall,
}

impl RedisReclaimStaleAggregationSetsPlan {
    pub fn from_queue(
        queue: &str,
        now: SystemTime,
    ) -> Result<Self, RedisReclaimStaleAggregationSetsPlanError> {
        // Reference: Asynq v0.26.0 `RDB.ReclaimStaleAggregationSets` derives
        // the all-aggregation-sets key directly from the supplied queue:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1256-L1261>.
        Ok(Self {
            call: RedisScriptCall::new(
                RedisScript::ReclaimStaleAggregationSets,
                vec![keys::all_aggregation_sets_key(queue)],
                vec![RedisArg::I64(unix_seconds_reclaim(
                    now,
                    "stale aggregation set scan",
                )?)],
            ),
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}

fn unix_seconds_aggregation(
    time: SystemTime,
    context: &'static str,
) -> Result<i64, RedisAggregationCheckPlanError> {
    let seconds = unix_seconds_floor(time);
    seconds
        .try_into()
        .map_err(|_| RedisAggregationCheckPlanError::TimeOverflow(context))
}

fn unix_seconds_reclaim(
    time: SystemTime,
    context: &'static str,
) -> Result<i64, RedisReclaimStaleAggregationSetsPlanError> {
    let seconds = unix_seconds_floor(time);
    seconds
        .try_into()
        .map_err(|_| RedisReclaimStaleAggregationSetsPlanError::TimeOverflow(context))
}

fn aggregation_duration_seconds(
    duration: Duration,
    context: &'static str,
) -> Result<i64, RedisAggregationCheckPlanError> {
    // Reference: Asynq v0.26.0 aggregation grace/max-delay values come from
    // Go `time.Duration` configuration and are passed to Redis as seconds:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1126-L1143>.
    if duration.as_nanos() > MAX_DURATION_NANOS_U128 {
        return Err(RedisAggregationCheckPlanError::TimeOverflow(context));
    }
    Ok(duration_seconds(duration))
}
