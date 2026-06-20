use super::*;

#[test]
fn plans_forward_scheduled_script_for_queue() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    let plan = RedisForwardPlan::from_scheduled_queue("critical", now).unwrap();
    let call = plan.call();

    assert_eq!(call.script(), RedisScript::Forward);
    assert_eq!(
        call.keys(),
        &[
            "asynq:{critical}:scheduled".to_owned(),
            "asynq:{critical}:pending".to_owned(),
        ]
    );
    assert_eq!(call.args()[0], RedisArg::I64(1_700_000_000));
    assert_eq!(
        call.args()[1],
        RedisArg::String("asynq:{critical}:t:".to_owned())
    );
    assert_eq!(call.args()[2], RedisArg::I64(1_700_000_000_000_000_000));
    assert_eq!(
        call.args()[3],
        RedisArg::String("asynq:{critical}:g:".to_owned())
    );
}

#[test]
fn plans_forward_retry_script_for_queue() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    let plan = RedisForwardPlan::from_retry_queue("critical", now).unwrap();
    let call = plan.call();

    assert_eq!(call.script(), RedisScript::Forward);
    assert_eq!(call.keys()[0], "asynq:{critical}:retry");
}

#[test]
fn plans_forward_allows_blank_queue() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    let scheduled = RedisForwardPlan::from_scheduled_queue(" ", now).unwrap();
    assert_eq!(
        scheduled.call().keys(),
        &[
            "asynq:{ }:scheduled".to_owned(),
            "asynq:{ }:pending".to_owned()
        ]
    );
    assert_eq!(
        scheduled.call().args(),
        &[
            RedisArg::I64(1_700_000_000),
            RedisArg::String("asynq:{ }:t:".to_owned()),
            RedisArg::I64(1_700_000_000_000_000_000),
            RedisArg::String("asynq:{ }:g:".to_owned()),
        ]
    );

    let retry = RedisForwardPlan::from_retry_queue(" ", now).unwrap();
    assert_eq!(
        retry.call().keys(),
        &["asynq:{ }:retry".to_owned(), "asynq:{ }:pending".to_owned()]
    );
}

#[test]
fn plans_delete_expired_completed_tasks_script_for_queue() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    let plan = RedisDeleteExpiredCompletedTasksPlan::from_queue("critical", now, 100).unwrap();
    let call = plan.call();

    assert_eq!(call.script(), RedisScript::DeleteExpiredCompletedTasks);
    assert_eq!(call.keys(), &["asynq:{critical}:completed".to_owned()]);
    assert_eq!(
        call.args(),
        &[
            RedisArg::I64(1_700_000_000),
            RedisArg::String("asynq:{critical}:t:".to_owned()),
            RedisArg::I64(100),
        ]
    );
}

#[test]
fn plans_delete_expired_completed_tasks_allows_blank_queue() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    let plan = RedisDeleteExpiredCompletedTasksPlan::from_queue(" ", now, 100).unwrap();

    assert_eq!(plan.call().keys(), &["asynq:{ }:completed".to_owned()]);
    assert_eq!(
        plan.call().args(),
        &[
            RedisArg::I64(1_700_000_000),
            RedisArg::String("asynq:{ }:t:".to_owned()),
            RedisArg::I64(100),
        ]
    );
}

#[test]
fn plans_delete_expired_completed_tasks_allows_zero_batch_size() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    let plan = RedisDeleteExpiredCompletedTasksPlan::from_queue("critical", now, 0).unwrap();

    assert_eq!(
        plan.call().args(),
        &[
            RedisArg::I64(1_700_000_000),
            RedisArg::String("asynq:{critical}:t:".to_owned()),
            RedisArg::I64(0),
        ]
    );
}

#[test]
fn plans_delete_expired_completed_tasks_preserves_signed_batch_size() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    let plan = RedisDeleteExpiredCompletedTasksPlan::from_queue("critical", now, -1).unwrap();

    assert_eq!(
        plan.call().args(),
        &[
            RedisArg::I64(1_700_000_000),
            RedisArg::String("asynq:{critical}:t:".to_owned()),
            RedisArg::I64(-1),
        ]
    );
}
