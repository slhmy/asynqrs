use super::*;

#[test]
fn redis_aggregation_plans_match_script_shapes() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    RedisAggregationCheckPlan::from_group(RedisAggregationCheckInput {
        queue: "critical",
        group: "tenant-a",
        set_id: "set-id".to_owned(),
        check_time: now,
        deadline_base: now,
        grace_period: Duration::from_secs(10),
        max_delay: Duration::from_secs(60),
        max_size: 100,
    })
    .unwrap()
    .call()
    .validate()
    .unwrap();
    RedisReadAggregationSetPlan::from_set("critical", "tenant-a", "set-id")
        .unwrap()
        .call()
        .validate()
        .unwrap();
    RedisDeleteAggregationSetPlan::from_set("critical", "tenant-a", "set-id")
        .unwrap()
        .call()
        .validate()
        .unwrap();
    RedisReclaimStaleAggregationSetsPlan::from_queue("critical", now)
        .unwrap()
        .call()
        .validate()
        .unwrap();
}
