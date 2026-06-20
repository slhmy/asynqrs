use super::*;

#[test]
fn plans_done_script_for_zero_retention_task() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let msg = active_message(0, "");

    let plan = RedisCompletePlan::from_message(&msg, now).unwrap();
    let call = plan.call();

    assert_eq!(call.script(), RedisScript::Done);
    assert_eq!(
        call.keys(),
        &[
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:lease".to_owned(),
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:processed:2023-11-14".to_owned(),
            "asynq:{critical}:processed".to_owned(),
        ]
    );
    assert_eq!(call.args()[0], RedisArg::String("task-id".to_owned()));
    assert_eq!(call.args()[1], RedisArg::I64(1_707_776_000));
    assert_eq!(call.args()[2], RedisArg::I64(i64::MAX));

    let mut blank_id = active_message(0, "");
    blank_id.id = String::new();
    let blank_plan = RedisCompletePlan::from_message(&blank_id, now).unwrap();
    assert_eq!(
        blank_plan.call().keys()[2],
        "asynq:{critical}:t:".to_owned()
    );
    assert_eq!(blank_plan.call().args()[0], RedisArg::String(String::new()));
}

#[test]
fn plans_done_unique_script_for_zero_retention_unique_task() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let msg = active_message(
        0,
        "asynq:{critical}:unique:email:welcome:321c3cf486ed509164edec1e1981fec8",
    );

    let plan = RedisCompletePlan::from_message(&msg, now).unwrap();
    let call = plan.call();

    assert_eq!(call.script(), RedisScript::DoneUnique);
    assert_eq!(
        call.keys()[5],
        "asynq:{critical}:unique:email:welcome:321c3cf486ed509164edec1e1981fec8"
    );
    assert_eq!(call.args()[0], RedisArg::String("task-id".to_owned()));
}

#[test]
fn plans_mark_as_complete_script_for_retained_task() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let msg = active_message(300, "");

    let plan = RedisCompletePlan::from_message(&msg, now).unwrap();
    let call = plan.call();

    assert_eq!(call.script(), RedisScript::MarkAsComplete);
    assert_eq!(
        call.keys(),
        &[
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:lease".to_owned(),
            "asynq:{critical}:completed".to_owned(),
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:processed:2023-11-14".to_owned(),
            "asynq:{critical}:processed".to_owned(),
        ]
    );
    assert_eq!(call.args()[0], RedisArg::String("task-id".to_owned()));
    assert_eq!(call.args()[1], RedisArg::I64(1_707_776_000));
    assert_eq!(call.args()[2], RedisArg::I64(1_700_000_300));
    assert_completed_message(&call.args()[3], &msg, 1_700_000_000);
    assert_eq!(call.args()[4], RedisArg::I64(i64::MAX));
}

#[test]
fn plans_mark_as_complete_wraps_retention_expiration() {
    let now = UNIX_EPOCH + Duration::from_secs(1);
    let msg = active_message(i64::MAX, "");

    let plan = RedisCompletePlan::from_message(&msg, now).unwrap();

    assert_eq!(plan.call().args()[2], RedisArg::I64(i64::MIN));
}

#[test]
fn plans_mark_as_complete_unique_script_for_retained_unique_task() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let msg = active_message(
        300,
        "asynq:{critical}:unique:email:welcome:321c3cf486ed509164edec1e1981fec8",
    );

    let plan = RedisCompletePlan::from_message(&msg, now).unwrap();
    let call = plan.call();

    assert_eq!(call.script(), RedisScript::MarkAsCompleteUnique);
    assert_eq!(
        call.keys()[6],
        "asynq:{critical}:unique:email:welcome:321c3cf486ed509164edec1e1981fec8"
    );
    assert_completed_message(&call.args()[3], &msg, 1_700_000_000);
}

#[test]
fn plans_complete_allows_blank_queue_and_task_id() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = active_message(0, "");

    msg.queue = " ".to_owned();
    let blank_queue = RedisCompletePlan::from_message(&msg, now).unwrap();
    assert_eq!(
        blank_queue.call().keys(),
        &[
            "asynq:{ }:active".to_owned(),
            "asynq:{ }:lease".to_owned(),
            "asynq:{ }:t:task-id".to_owned(),
            "asynq:{ }:processed:2023-11-14".to_owned(),
            "asynq:{ }:processed".to_owned(),
        ]
    );

    msg.queue = "critical".to_owned();
    msg.id = " ".to_owned();
    assert!(RedisCompletePlan::from_message(&msg, now).is_ok());
}
