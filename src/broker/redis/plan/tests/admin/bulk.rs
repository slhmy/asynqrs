use super::*;

#[test]
fn plans_bulk_task_admin_commands() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    let run_all_scheduled =
        RedisRunAllTasksPlan::from_queue_and_state("critical", TaskState::Scheduled).unwrap();
    let call = run_all_scheduled.call();
    assert_eq!(call.script(), RedisScript::RunAllTasks);
    assert_eq!(
        call.keys(),
        &[
            "asynq:{critical}:scheduled".to_owned(),
            "asynq:{critical}:pending".to_owned(),
        ]
    );
    assert_eq!(
        call.args(),
        &[RedisArg::String("asynq:{critical}:t:".to_owned())]
    );

    let run_all_retry =
        RedisRunAllTasksPlan::from_queue_and_state("critical", TaskState::Retry).unwrap();
    assert_eq!(
        run_all_retry.call().keys(),
        &[
            "asynq:{critical}:retry".to_owned(),
            "asynq:{critical}:pending".to_owned(),
        ]
    );

    let run_all_archived =
        RedisRunAllTasksPlan::from_queue_and_state("critical", TaskState::Archived).unwrap();
    assert_eq!(
        run_all_archived.call().keys(),
        &[
            "asynq:{critical}:archived".to_owned(),
            "asynq:{critical}:pending".to_owned(),
        ]
    );

    let archive_all_pending =
        RedisArchiveAllTasksPlan::from_queue_state_and_time("critical", TaskState::Pending, now)
            .unwrap();
    let call = archive_all_pending.call();
    assert_eq!(call.script(), RedisScript::ArchiveAllPendingTasks);
    assert_eq!(
        call.keys(),
        &[
            "asynq:{critical}:pending".to_owned(),
            "asynq:{critical}:archived".to_owned(),
        ]
    );
    assert_eq!(
        call.args(),
        &[
            RedisArg::I64(1_700_000_000),
            RedisArg::I64(1_692_224_000),
            RedisArg::I64(10_000),
            RedisArg::String("asynq:{critical}:t:".to_owned()),
        ]
    );

    let archive_all_scheduled =
        RedisArchiveAllTasksPlan::from_queue_state_and_time("critical", TaskState::Scheduled, now)
            .unwrap();
    assert_eq!(
        archive_all_scheduled.call().script(),
        RedisScript::ArchiveAllTasks
    );
    assert_eq!(
        archive_all_scheduled.call().keys(),
        &[
            "asynq:{critical}:scheduled".to_owned(),
            "asynq:{critical}:archived".to_owned(),
        ]
    );

    let archive_all_retry =
        RedisArchiveAllTasksPlan::from_queue_state_and_time("critical", TaskState::Retry, now)
            .unwrap();
    assert_eq!(
        archive_all_retry.call().keys(),
        &[
            "asynq:{critical}:retry".to_owned(),
            "asynq:{critical}:archived".to_owned(),
        ]
    );

    let delete_all_pending =
        RedisDeleteAllTasksPlan::from_queue_and_state("critical", TaskState::Pending).unwrap();
    let call = delete_all_pending.call();
    assert_eq!(call.script(), RedisScript::DeleteAllPendingTasks);
    assert_eq!(call.keys(), &["asynq:{critical}:pending".to_owned()]);
    assert_eq!(
        call.args(),
        &[RedisArg::String("asynq:{critical}:t:".to_owned())]
    );

    let delete_all_scheduled =
        RedisDeleteAllTasksPlan::from_queue_and_state("critical", TaskState::Scheduled).unwrap();
    assert_eq!(
        delete_all_scheduled.call().script(),
        RedisScript::DeleteAllTasks
    );
    assert_eq!(
        delete_all_scheduled.call().keys(),
        &["asynq:{critical}:scheduled".to_owned()]
    );

    let delete_all_retry =
        RedisDeleteAllTasksPlan::from_queue_and_state("critical", TaskState::Retry).unwrap();
    assert_eq!(
        delete_all_retry.call().keys(),
        &["asynq:{critical}:retry".to_owned()]
    );

    let delete_all_archived =
        RedisDeleteAllTasksPlan::from_queue_and_state("critical", TaskState::Archived).unwrap();
    assert_eq!(
        delete_all_archived.call().keys(),
        &["asynq:{critical}:archived".to_owned()]
    );

    let delete_all_completed =
        RedisDeleteAllTasksPlan::from_queue_and_state("critical", TaskState::Completed).unwrap();
    assert_eq!(
        delete_all_completed.call().keys(),
        &["asynq:{critical}:completed".to_owned()]
    );
}
