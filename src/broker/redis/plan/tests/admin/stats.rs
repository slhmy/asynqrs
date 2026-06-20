use super::*;

#[test]
fn plans_admin_queue_stats_commands() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    let stats = RedisCurrentQueueStatsPlan::from_queue("critical", now).unwrap();
    let call = stats.call();
    assert_eq!(stats.queue(), "critical");
    assert_eq!(call.script(), RedisScript::CurrentQueueStats);
    assert_eq!(
        call.keys(),
        &[
            "asynq:{critical}:pending".to_owned(),
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:scheduled".to_owned(),
            "asynq:{critical}:retry".to_owned(),
            "asynq:{critical}:archived".to_owned(),
            "asynq:{critical}:completed".to_owned(),
            "asynq:{critical}:processed:2023-11-14".to_owned(),
            "asynq:{critical}:failed:2023-11-14".to_owned(),
            "asynq:{critical}:processed".to_owned(),
            "asynq:{critical}:failed".to_owned(),
            "asynq:{critical}:paused".to_owned(),
            "asynq:{critical}:groups".to_owned(),
        ]
    );
    assert_eq!(
        call.args(),
        &[
            RedisArg::String("asynq:{critical}:t:".to_owned()),
            RedisArg::String("asynq:{critical}:g:".to_owned()),
        ]
    );

    let memory = RedisQueueMemoryUsagePlan::from_queue("critical").unwrap();
    let call = memory.call();
    assert_eq!(call.script(), RedisScript::QueueMemoryUsage);
    assert_eq!(call.keys()[0], "asynq:{critical}:active");
    assert_eq!(call.keys()[6], "asynq:{critical}:groups");
    assert_eq!(
        call.args(),
        &[
            RedisArg::String("asynq:{critical}:t:".to_owned()),
            RedisArg::I64(20),
            RedisArg::I64(5),
            RedisArg::String("asynq:{critical}:g:".to_owned()),
        ]
    );

    let historical = RedisHistoricalQueueStatsPlan::from_queue("critical", now, 2).unwrap();
    let call = historical.call();
    assert_eq!(historical.queue(), "critical");
    assert_eq!(historical.days().len(), 2);
    assert_eq!(call.script(), RedisScript::HistoricalQueueStats);
    assert_eq!(
        call.keys(),
        &[
            "asynq:{critical}:processed:2023-11-14".to_owned(),
            "asynq:{critical}:failed:2023-11-14".to_owned(),
            "asynq:{critical}:processed:2023-11-13".to_owned(),
            "asynq:{critical}:failed:2023-11-13".to_owned(),
        ]
    );
    assert!(call.args().is_empty());

    let groups = RedisGroupStatsPlan::from_queue("critical").unwrap();
    let call = groups.call();
    assert_eq!(call.script(), RedisScript::GroupStats);
    assert_eq!(call.keys(), &["asynq:{critical}:groups".to_owned()]);
    assert_eq!(
        call.args(),
        &[RedisArg::String("asynq:{critical}:g:".to_owned())]
    );

    let list_groups = RedisListAggregationGroupsPlan::from_queue("critical").unwrap();
    assert_eq!(list_groups.key(), "asynq:{critical}:groups");

    let blank_group_stats = RedisGroupStatsPlan::from_queue(" ").unwrap();
    assert_eq!(
        blank_group_stats.call().keys(),
        &["asynq:{ }:groups".to_owned()]
    );
    assert_eq!(
        blank_group_stats.call().args(),
        &[RedisArg::String("asynq:{ }:g:".to_owned())]
    );

    let blank_list_groups = RedisListAggregationGroupsPlan::from_queue(" ").unwrap();
    assert_eq!(blank_list_groups.key(), "asynq:{ }:groups");
}
