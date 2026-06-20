use crate::broker::redis::RedisScript;

use super::super::sources::*;
use super::super::{RedisScriptArgShape, RedisScriptSpec};

pub(super) const fn spec(script: RedisScript) -> RedisScriptSpec {
    match script {
        RedisScript::AggregationCheck => RedisScriptSpec::new(
            script,
            "aggregation_check",
            AGGREGATION_CHECK_SOURCE,
            4,
            RedisScriptArgShape::Exact(6),
        ),
        RedisScript::ReadAggregationSet => RedisScriptSpec::new(
            script,
            "read_aggregation_set",
            READ_AGGREGATION_SET_SOURCE,
            1,
            RedisScriptArgShape::Exact(1),
        ),
        RedisScript::DeleteAggregationSet => RedisScriptSpec::new(
            script,
            "delete_aggregation_set",
            DELETE_AGGREGATION_SET_SOURCE,
            2,
            RedisScriptArgShape::Exact(1),
        ),
        RedisScript::ReclaimStaleAggregationSets => RedisScriptSpec::new(
            script,
            "reclaim_stale_aggregation_sets",
            RECLAIM_STALE_AGGREGATION_SETS_SOURCE,
            1,
            RedisScriptArgShape::Exact(1),
        ),
        _ => panic!("unsupported aggregation script"),
    }
}
