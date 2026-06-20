use super::*;

#[test]
fn plans_task_info_admin_command() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    let info = RedisTaskInfoPlan::from_queue_task_id_and_time("critical", "task-id", now).unwrap();
    assert_eq!(info.call().script(), RedisScript::TaskInfo);
    assert_eq!(
        info.call().keys(),
        &["asynq:{critical}:t:task-id".to_owned()]
    );
    assert_eq!(
        info.call().args(),
        &[
            RedisArg::String("task-id".to_owned()),
            RedisArg::I64(1_700_000_000),
            RedisArg::String("asynq:{critical}:".to_owned()),
        ]
    );
    let blank_info = RedisTaskInfoPlan::from_queue_task_id_and_time("critical", "", now).unwrap();
    assert_eq!(
        blank_info.call().keys(),
        &["asynq:{critical}:t:".to_owned()]
    );
    assert_eq!(blank_info.call().args()[0], RedisArg::String(String::new()));

    let blank_queue_info =
        RedisTaskInfoPlan::from_queue_task_id_and_time(" ", "task-id", now).unwrap();
    assert_eq!(
        blank_queue_info.call().keys(),
        &["asynq:{ }:t:task-id".to_owned()]
    );
    assert_eq!(
        blank_queue_info.call().args(),
        &[
            RedisArg::String("task-id".to_owned()),
            RedisArg::I64(1_700_000_000),
            RedisArg::String("asynq:{ }:".to_owned()),
        ]
    );
}
