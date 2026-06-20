use super::*;

#[test]
fn plans_retry_script_for_failed_active_task() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let retry_at = now + Duration::from_secs(60);
    let msg = active_message(0, "");

    let plan = RedisRetryPlan::from_message(&msg, now, retry_at, "handler failed", true).unwrap();
    let call = plan.call();

    assert_eq!(call.script(), RedisScript::Retry);
    assert_eq!(
        call.keys(),
        &[
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:lease".to_owned(),
            "asynq:{critical}:retry".to_owned(),
            "asynq:{critical}:processed:2023-11-14".to_owned(),
            "asynq:{critical}:failed:2023-11-14".to_owned(),
            "asynq:{critical}:processed".to_owned(),
            "asynq:{critical}:failed".to_owned(),
        ]
    );
    assert_eq!(call.args()[0], RedisArg::String("task-id".to_owned()));
    assert_retry_message(&call.args()[1], &msg, "handler failed", 1_700_000_000);
    assert_eq!(call.args()[2], RedisArg::I64(1_700_000_060));
    assert_eq!(call.args()[3], RedisArg::I64(1_707_776_000));
    assert_eq!(call.args()[4], RedisArg::String("1".to_owned()));
    assert_eq!(call.args()[5], RedisArg::I64(i64::MAX));

    let mut blank_id = active_message(0, "");
    blank_id.id = String::new();
    let blank_plan =
        RedisRetryPlan::from_message(&blank_id, now, retry_at, "handler failed", true).unwrap();
    assert_eq!(
        blank_plan.call().keys()[0],
        "asynq:{critical}:t:".to_owned()
    );
    assert_eq!(blank_plan.call().args()[0], RedisArg::String(String::new()));
}

#[test]
fn plans_retry_without_failure_counting() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let retry_at = now + Duration::from_secs(60);
    let msg = active_message(0, "");

    let plan = RedisRetryPlan::from_message(&msg, now, retry_at, "transient", false).unwrap();
    let call = plan.call();

    assert_retry_message_with_retried(&call.args()[1], &msg, "transient", 1_700_000_000, 0);
    assert_eq!(call.args()[4], RedisArg::String("0".to_owned()));
}

#[test]
fn plans_retry_uses_upstream_int32_conversion_for_retried_increment() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let retry_at = now + Duration::from_secs(60);
    let mut msg = active_message(0, "");
    msg.retried = i32::MAX;

    let plan = RedisRetryPlan::from_message(&msg, now, retry_at, "overflow", true).unwrap();

    assert_retry_message_with_retried(
        &plan.call().args()[1],
        &msg,
        "overflow",
        1_700_000_000,
        i32::MIN,
    );
}

#[test]
fn plans_retry_allows_blank_queue_and_task_id() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut msg = active_message(0, "");

    msg.queue = " ".to_owned();
    let blank_queue = RedisRetryPlan::from_message(&msg, now, now, "handler failed", true).unwrap();
    assert_eq!(
        blank_queue.call().keys(),
        &[
            "asynq:{ }:t:task-id".to_owned(),
            "asynq:{ }:active".to_owned(),
            "asynq:{ }:lease".to_owned(),
            "asynq:{ }:retry".to_owned(),
            "asynq:{ }:processed:2023-11-14".to_owned(),
            "asynq:{ }:failed:2023-11-14".to_owned(),
            "asynq:{ }:processed".to_owned(),
            "asynq:{ }:failed".to_owned(),
        ]
    );

    msg.queue = "critical".to_owned();
    msg.id = " ".to_owned();
    assert!(RedisRetryPlan::from_message(&msg, now, now, "handler failed", true).is_ok());
}
