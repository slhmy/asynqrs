use super::*;

#[test]
fn plans_archive_script_for_failed_active_task() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let msg = active_message(0, "");

    let plan = RedisArchivePlan::from_message(&msg, now, "max retry exhausted").unwrap();
    let call = plan.call();

    assert_eq!(call.script(), RedisScript::Archive);
    assert_eq!(
        call.keys(),
        &[
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:lease".to_owned(),
            "asynq:{critical}:archived".to_owned(),
            "asynq:{critical}:processed:2023-11-14".to_owned(),
            "asynq:{critical}:failed:2023-11-14".to_owned(),
            "asynq:{critical}:processed".to_owned(),
            "asynq:{critical}:failed".to_owned(),
            "asynq:{critical}:t:".to_owned(),
        ]
    );
    assert_eq!(call.args()[0], RedisArg::String("task-id".to_owned()));
    assert_failure_message(&call.args()[1], &msg, "max retry exhausted", 1_700_000_000);
    assert_eq!(call.args()[2], RedisArg::I64(1_700_000_000));
    assert_eq!(call.args()[3], RedisArg::I64(1_692_224_000));
    assert_eq!(call.args()[4], RedisArg::I64(10_000));
    assert_eq!(call.args()[5], RedisArg::I64(1_707_776_000));
    assert_eq!(call.args()[6], RedisArg::I64(i64::MAX));

    let mut blank_id = active_message(0, "");
    blank_id.id = String::new();
    let blank_plan = RedisArchivePlan::from_message(&blank_id, now, "max retry exhausted").unwrap();
    assert_eq!(
        blank_plan.call().keys()[0],
        "asynq:{critical}:t:".to_owned()
    );
    assert_eq!(blank_plan.call().args()[0], RedisArg::String(String::new()));
}

#[test]
fn plans_archive_allows_blank_queue_and_task_id() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = active_message(0, "");

    msg.queue = " ".to_owned();
    let blank_queue = RedisArchivePlan::from_message(&msg, now, "max retry exhausted").unwrap();
    assert_eq!(
        blank_queue.call().keys(),
        &[
            "asynq:{ }:t:task-id".to_owned(),
            "asynq:{ }:active".to_owned(),
            "asynq:{ }:lease".to_owned(),
            "asynq:{ }:archived".to_owned(),
            "asynq:{ }:processed:2023-11-14".to_owned(),
            "asynq:{ }:failed:2023-11-14".to_owned(),
            "asynq:{ }:processed".to_owned(),
            "asynq:{ }:failed".to_owned(),
            "asynq:{ }:t:".to_owned(),
        ]
    );

    msg.queue = "critical".to_owned();
    msg.id = " ".to_owned();
    assert!(RedisArchivePlan::from_message(&msg, now, "max retry exhausted").is_ok());
}
