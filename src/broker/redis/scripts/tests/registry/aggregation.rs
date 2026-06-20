use super::*;

#[test]
fn aggregation_scripts_have_sources_and_shapes() {
    let source = assert_script_shape(RedisScript::AggregationCheck, "aggregation_check", 4, 6);
    assert!(source.contains("ZCARD"));
    assert!(source.contains("ZREMRANGEBYRANK"));

    let source = assert_script_shape(
        RedisScript::ReadAggregationSet,
        "read_aggregation_set",
        1,
        1,
    );
    assert!(source.contains("ZRANGE"));
    assert!(source.contains("HGET"));

    let source = assert_script_shape(
        RedisScript::DeleteAggregationSet,
        "delete_aggregation_set",
        2,
        1,
    );
    assert!(source.contains("DEL"));
    assert!(source.contains("status_reply"));

    let source = assert_script_shape(
        RedisScript::ReclaimStaleAggregationSets,
        "reclaim_stale_aggregation_sets",
        1,
        1,
    );
    assert!(source.contains("ZRANGEBYSCORE"));
    assert!(source.contains("ZREMRANGEBYSCORE"));
}
