use super::*;

#[test]
fn plans_aggregation_check_script_for_group() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    let plan = RedisAggregationCheckPlan::from_group(RedisAggregationCheckInput {
        queue: "critical",
        group: "tenant-a",
        set_id: "set-id".to_owned(),
        check_time: now,
        deadline_base: now,
        grace_period: Duration::from_secs(10),
        max_delay: Duration::from_secs(60),
        max_size: 100,
    })
    .unwrap();
    let call = plan.call();

    assert_eq!(plan.set_id(), "set-id");
    assert_eq!(call.script(), RedisScript::AggregationCheck);
    assert_eq!(
        call.keys(),
        &[
            "asynq:{critical}:g:tenant-a".to_owned(),
            "asynq:{critical}:g:tenant-a:set-id".to_owned(),
            "asynq:{critical}:aggregation_sets".to_owned(),
            "asynq:{critical}:groups".to_owned(),
        ]
    );
    assert_eq!(
        call.args(),
        &[
            RedisArg::I64(100),
            RedisArg::I64(60),
            RedisArg::I64(10),
            RedisArg::I64(1_700_000_120),
            RedisArg::I64(1_700_000_000),
            RedisArg::String("tenant-a".to_owned()),
        ]
    );

    let blank_parts = RedisAggregationCheckPlan::from_group(RedisAggregationCheckInput {
        queue: "critical",
        group: " ",
        set_id: String::new(),
        check_time: now,
        deadline_base: now,
        grace_period: Duration::from_secs(10),
        max_delay: Duration::from_secs(60),
        max_size: 100,
    })
    .unwrap();
    assert_eq!(blank_parts.set_id(), "");
    assert_eq!(
        blank_parts.call().keys(),
        &[
            "asynq:{critical}:g: ".to_owned(),
            "asynq:{critical}:g: :".to_owned(),
            "asynq:{critical}:aggregation_sets".to_owned(),
            "asynq:{critical}:groups".to_owned(),
        ]
    );
    assert_eq!(
        blank_parts.call().args()[5],
        RedisArg::String(" ".to_owned())
    );

    assert_eq!(
        RedisAggregationCheckPlan::from_group(RedisAggregationCheckInput {
            queue: "critical",
            group: "tenant-a",
            set_id: "set-id".to_owned(),
            check_time: now,
            deadline_base: now,
            grace_period: too_large_go_duration(),
            max_delay: Duration::from_secs(60),
            max_size: 100,
        })
        .unwrap_err(),
        RedisAggregationCheckPlanError::TimeOverflow("aggregation grace period")
    );
    assert_eq!(
        RedisAggregationCheckPlan::from_group(RedisAggregationCheckInput {
            queue: "critical",
            group: "tenant-a",
            set_id: "set-id".to_owned(),
            check_time: now,
            deadline_base: now,
            grace_period: Duration::from_secs(10),
            max_delay: too_large_go_duration(),
            max_size: 100,
        })
        .unwrap_err(),
        RedisAggregationCheckPlanError::TimeOverflow("aggregation max delay")
    );
}

#[test]
fn plans_aggregation_check_allows_blank_queue() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    let blank_queue = RedisAggregationCheckPlan::from_group(RedisAggregationCheckInput {
        queue: " ",
        group: "tenant-a",
        set_id: "set-id".to_owned(),
        check_time: now,
        deadline_base: now,
        grace_period: Duration::from_secs(10),
        max_delay: Duration::from_secs(60),
        max_size: 100,
    })
    .unwrap();

    assert_eq!(
        blank_queue.call().keys(),
        &[
            "asynq:{ }:g:tenant-a".to_owned(),
            "asynq:{ }:g:tenant-a:set-id".to_owned(),
            "asynq:{ }:aggregation_sets".to_owned(),
            "asynq:{ }:groups".to_owned(),
        ]
    );
}

#[test]
fn plans_aggregation_check_preserves_signed_max_size() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    let plan = RedisAggregationCheckPlan::from_group(RedisAggregationCheckInput {
        queue: "critical",
        group: "tenant-a",
        set_id: "set-id".to_owned(),
        check_time: now,
        deadline_base: now,
        grace_period: Duration::from_secs(10),
        max_delay: Duration::from_secs(60),
        max_size: -1,
    })
    .unwrap();

    assert_eq!(plan.call().args()[0], RedisArg::I64(-1));
}

#[test]
fn plans_read_aggregation_set_script() {
    let plan = RedisReadAggregationSetPlan::from_set("critical", "tenant-a", "set-id").unwrap();
    let call = plan.call();

    assert_eq!(call.script(), RedisScript::ReadAggregationSet);
    assert_eq!(
        call.keys(),
        &["asynq:{critical}:g:tenant-a:set-id".to_owned()]
    );
    assert_eq!(
        call.args(),
        &[RedisArg::String("asynq:{critical}:t:".to_owned())]
    );
    assert_eq!(
        plan.aggregation_set_key(),
        "asynq:{critical}:g:tenant-a:set-id"
    );
    assert_eq!(
        plan.all_aggregation_sets_key(),
        "asynq:{critical}:aggregation_sets"
    );

    let blank_parts = RedisReadAggregationSetPlan::from_set("critical", " ", "").unwrap();
    assert_eq!(
        blank_parts.call().keys(),
        &["asynq:{critical}:g: :".to_owned()]
    );
    assert_eq!(blank_parts.aggregation_set_key(), "asynq:{critical}:g: :");
}

#[test]
fn plans_read_aggregation_set_allows_blank_queue() {
    let blank_queue = RedisReadAggregationSetPlan::from_set(" ", "tenant-a", "set-id").unwrap();

    assert_eq!(
        blank_queue.call().keys(),
        &["asynq:{ }:g:tenant-a:set-id".to_owned()]
    );
    assert_eq!(
        blank_queue.call().args(),
        &[RedisArg::String("asynq:{ }:t:".to_owned())]
    );
    assert_eq!(
        blank_queue.all_aggregation_sets_key(),
        "asynq:{ }:aggregation_sets"
    );
}

#[test]
fn plans_delete_aggregation_set_script() {
    let plan = RedisDeleteAggregationSetPlan::from_set("critical", "tenant-a", "set-id").unwrap();
    let call = plan.call();

    assert_eq!(call.script(), RedisScript::DeleteAggregationSet);
    assert_eq!(
        call.keys(),
        &[
            "asynq:{critical}:g:tenant-a:set-id".to_owned(),
            "asynq:{critical}:aggregation_sets".to_owned(),
        ]
    );
    assert_eq!(
        call.args(),
        &[RedisArg::String("asynq:{critical}:t:".to_owned())]
    );

    let blank_parts = RedisDeleteAggregationSetPlan::from_set("critical", " ", "").unwrap();
    assert_eq!(
        blank_parts.call().keys(),
        &[
            "asynq:{critical}:g: :".to_owned(),
            "asynq:{critical}:aggregation_sets".to_owned(),
        ]
    );
}

#[test]
fn plans_delete_aggregation_set_allows_blank_queue() {
    let blank_queue = RedisDeleteAggregationSetPlan::from_set(" ", "tenant-a", "set-id").unwrap();

    assert_eq!(
        blank_queue.call().keys(),
        &[
            "asynq:{ }:g:tenant-a:set-id".to_owned(),
            "asynq:{ }:aggregation_sets".to_owned(),
        ]
    );
    assert_eq!(
        blank_queue.call().args(),
        &[RedisArg::String("asynq:{ }:t:".to_owned())]
    );
}

#[test]
fn plans_reclaim_stale_aggregation_sets_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    let plan = RedisReclaimStaleAggregationSetsPlan::from_queue("critical", now).unwrap();
    let call = plan.call();

    assert_eq!(call.script(), RedisScript::ReclaimStaleAggregationSets);
    assert_eq!(
        call.keys(),
        &["asynq:{critical}:aggregation_sets".to_owned()]
    );
    assert_eq!(call.args(), &[RedisArg::I64(1_700_000_000)]);
}

#[test]
fn plans_reclaim_stale_aggregation_sets_allows_blank_queue() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    let blank_queue = RedisReclaimStaleAggregationSetsPlan::from_queue(" ", now).unwrap();

    assert_eq!(
        blank_queue.call().keys(),
        &["asynq:{ }:aggregation_sets".to_owned()]
    );
    assert_eq!(blank_queue.call().args(), &[RedisArg::I64(1_700_000_000)]);
}
