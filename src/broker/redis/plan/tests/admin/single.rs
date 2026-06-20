use super::*;

#[test]
fn plans_single_task_admin_commands() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    let delete_task = RedisDeleteTaskPlan::from_queue_and_task_id("critical", "task-id").unwrap();
    let call = delete_task.call();
    assert_eq!(call.script(), RedisScript::DeleteTask);
    assert_eq!(
        call.keys(),
        &[
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:groups".to_owned(),
        ]
    );
    assert_eq!(
        call.args(),
        &[
            RedisArg::String("task-id".to_owned()),
            RedisArg::String("asynq:{critical}:".to_owned()),
            RedisArg::String("asynq:{critical}:g:".to_owned()),
        ]
    );
    let delete_blank_task = RedisDeleteTaskPlan::from_queue_and_task_id("critical", "").unwrap();
    assert_eq!(
        delete_blank_task.call().keys(),
        &[
            "asynq:{critical}:t:".to_owned(),
            "asynq:{critical}:groups".to_owned(),
        ]
    );
    assert_eq!(
        delete_blank_task.call().args()[0],
        RedisArg::String(String::new())
    );

    let run_task = RedisRunTaskPlan::from_queue_and_task_id("critical", "task-id").unwrap();
    let call = run_task.call();
    assert_eq!(call.script(), RedisScript::RunTask);
    assert_eq!(
        call.keys(),
        &[
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:pending".to_owned(),
            "asynq:{critical}:groups".to_owned(),
        ]
    );
    assert_eq!(
        call.args(),
        &[
            RedisArg::String("task-id".to_owned()),
            RedisArg::String("asynq:{critical}:".to_owned()),
            RedisArg::String("asynq:{critical}:g:".to_owned()),
        ]
    );
    let run_blank_task = RedisRunTaskPlan::from_queue_and_task_id("critical", "").unwrap();
    assert_eq!(
        run_blank_task.call().keys(),
        &[
            "asynq:{critical}:t:".to_owned(),
            "asynq:{critical}:pending".to_owned(),
            "asynq:{critical}:groups".to_owned(),
        ]
    );
    assert_eq!(
        run_blank_task.call().args()[0],
        RedisArg::String(String::new())
    );

    let archive_task =
        RedisArchiveTaskPlan::from_queue_task_id_and_time("critical", "task-id", now).unwrap();
    let call = archive_task.call();
    assert_eq!(call.script(), RedisScript::ArchiveTask);
    assert_eq!(
        call.keys(),
        &[
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:archived".to_owned(),
            "asynq:{critical}:groups".to_owned(),
        ]
    );
    assert_eq!(
        call.args(),
        &[
            RedisArg::String("task-id".to_owned()),
            RedisArg::I64(1_700_000_000),
            RedisArg::I64(1_692_224_000),
            RedisArg::I64(10_000),
            RedisArg::String("asynq:{critical}:".to_owned()),
            RedisArg::String("asynq:{critical}:g:".to_owned()),
        ]
    );
    let archive_blank_task =
        RedisArchiveTaskPlan::from_queue_task_id_and_time("critical", "", now).unwrap();
    assert_eq!(
        archive_blank_task.call().keys(),
        &[
            "asynq:{critical}:t:".to_owned(),
            "asynq:{critical}:archived".to_owned(),
            "asynq:{critical}:groups".to_owned(),
        ]
    );
    assert_eq!(
        archive_blank_task.call().args()[0],
        RedisArg::String(String::new())
    );

    let mut updated_message =
        TaskMessage::from_task(&Task::new("email:welcome", b"updated".to_vec()));
    updated_message.id = "task-id".to_owned();
    updated_message.queue = "critical".to_owned();
    let update_payload = RedisUpdateTaskPayloadPlan::from_queue_task_id_and_message(
        "critical",
        "task-id",
        &updated_message,
    )
    .unwrap();
    let call = update_payload.call();
    assert_eq!(call.script(), RedisScript::UpdateTaskPayload);
    assert_eq!(call.keys(), &["asynq:{critical}:t:task-id".to_owned()]);
    assert_eq!(
        call.args(),
        &[RedisArg::Bytes(updated_message.encode_to_vec())]
    );
    let update_blank_task = RedisUpdateTaskPayloadPlan::from_queue_task_id_and_message(
        "critical",
        "",
        &updated_message,
    )
    .unwrap();
    assert_eq!(
        update_blank_task.call().keys(),
        &["asynq:{critical}:t:".to_owned()]
    );
}
