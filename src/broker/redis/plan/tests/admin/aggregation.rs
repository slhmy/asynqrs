use super::*;

#[test]
fn plans_aggregating_task_admin_commands() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    let run_aggregating =
        RedisRunAllAggregatingTasksPlan::from_queue_and_group("critical", "tenant-a").unwrap();
    let call = run_aggregating.call();
    assert_eq!(call.script(), RedisScript::RunAllAggregatingTasks);
    assert_eq!(
        call.keys(),
        &[
            "asynq:{critical}:g:tenant-a".to_owned(),
            "asynq:{critical}:pending".to_owned(),
            "asynq:{critical}:groups".to_owned(),
        ]
    );
    assert_eq!(
        call.args(),
        &[
            RedisArg::String("asynq:{critical}:t:".to_owned()),
            RedisArg::String("tenant-a".to_owned()),
        ]
    );
    let run_aggregating_blank_group =
        RedisRunAllAggregatingTasksPlan::from_queue_and_group("critical", " ").unwrap();
    assert_eq!(
        run_aggregating_blank_group.call().keys(),
        &[
            "asynq:{critical}:g: ".to_owned(),
            "asynq:{critical}:pending".to_owned(),
            "asynq:{critical}:groups".to_owned(),
        ]
    );
    assert_eq!(
        run_aggregating_blank_group.call().args(),
        &[
            RedisArg::String("asynq:{critical}:t:".to_owned()),
            RedisArg::String(" ".to_owned()),
        ]
    );

    let archive_aggregating =
        RedisArchiveAllAggregatingTasksPlan::from_queue_group_and_time("critical", "tenant-a", now)
            .unwrap();
    let call = archive_aggregating.call();
    assert_eq!(call.script(), RedisScript::ArchiveAllAggregatingTasks);
    assert_eq!(
        call.keys(),
        &[
            "asynq:{critical}:g:tenant-a".to_owned(),
            "asynq:{critical}:archived".to_owned(),
            "asynq:{critical}:groups".to_owned(),
        ]
    );
    assert_eq!(
        call.args(),
        &[
            RedisArg::I64(1_700_000_000),
            RedisArg::I64(1_692_224_000),
            RedisArg::I64(10_000),
            RedisArg::String("asynq:{critical}:t:".to_owned()),
            RedisArg::String("tenant-a".to_owned()),
        ]
    );
    let archive_aggregating_blank_group =
        RedisArchiveAllAggregatingTasksPlan::from_queue_group_and_time("critical", " ", now)
            .unwrap();
    assert_eq!(
        archive_aggregating_blank_group.call().keys(),
        &[
            "asynq:{critical}:g: ".to_owned(),
            "asynq:{critical}:archived".to_owned(),
            "asynq:{critical}:groups".to_owned(),
        ]
    );
    assert_eq!(
        archive_aggregating_blank_group.call().args()[4],
        RedisArg::String(" ".to_owned())
    );

    let delete_aggregating =
        RedisDeleteAllAggregatingTasksPlan::from_queue_and_group("critical", "tenant-a").unwrap();
    let call = delete_aggregating.call();
    assert_eq!(call.script(), RedisScript::DeleteAllAggregatingTasks);
    assert_eq!(
        call.keys(),
        &[
            "asynq:{critical}:g:tenant-a".to_owned(),
            "asynq:{critical}:groups".to_owned(),
        ]
    );
    assert_eq!(
        call.args(),
        &[
            RedisArg::String("asynq:{critical}:t:".to_owned()),
            RedisArg::String("tenant-a".to_owned()),
        ]
    );
    let delete_aggregating_blank_group =
        RedisDeleteAllAggregatingTasksPlan::from_queue_and_group("critical", " ").unwrap();
    assert_eq!(
        delete_aggregating_blank_group.call().keys(),
        &[
            "asynq:{critical}:g: ".to_owned(),
            "asynq:{critical}:groups".to_owned(),
        ]
    );
    assert_eq!(
        delete_aggregating_blank_group.call().args(),
        &[
            RedisArg::String("asynq:{critical}:t:".to_owned()),
            RedisArg::String(" ".to_owned()),
        ]
    );
}
