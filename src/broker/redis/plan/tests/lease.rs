use super::*;

#[test]
fn plans_recover_script_for_expired_leases() {
    let cutoff = UNIX_EPOCH + Duration::from_secs(1_699_999_970);

    let plan = RedisRecoverPlan::from_queue_and_cutoff("critical", cutoff).unwrap();
    let call = plan.call();

    assert_eq!(call.script(), RedisScript::ListLeaseExpired);
    assert_eq!(call.keys(), &["asynq:{critical}:lease".to_owned()]);
    assert_eq!(
        call.args(),
        &[
            RedisArg::I64(1_699_999_970),
            RedisArg::String("asynq:{critical}:t:".to_owned()),
        ]
    );
}

#[test]
fn plans_recover_script_accepts_recoverer_cutoff() {
    let cutoff = UNIX_EPOCH + Duration::from_secs(1_699_999_970);

    let plan = RedisRecoverPlan::from_queue_and_cutoff("critical", cutoff).unwrap();
    let call = plan.call();

    assert_eq!(call.script(), RedisScript::ListLeaseExpired);
    assert_eq!(call.keys(), &["asynq:{critical}:lease".to_owned()]);
    assert_eq!(
        call.args(),
        &[
            RedisArg::I64(1_699_999_970),
            RedisArg::String("asynq:{critical}:t:".to_owned()),
        ]
    );
}

#[test]
fn plans_recover_allows_blank_queue() {
    let cutoff = UNIX_EPOCH + Duration::from_secs(1_699_999_970);
    let plan = RedisRecoverPlan::from_queue_and_cutoff(" ", cutoff).unwrap();
    let call = plan.call();

    assert_eq!(call.script(), RedisScript::ListLeaseExpired);
    assert_eq!(call.keys(), &["asynq:{ }:lease".to_owned()]);
    assert_eq!(
        call.args(),
        &[
            RedisArg::I64(1_699_999_970),
            RedisArg::String("asynq:{ }:t:".to_owned()),
        ]
    );
}

#[test]
fn plans_extend_lease_command() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    let plan =
        RedisExtendLeasePlan::from_queue_and_task_ids("critical", &["task-id".to_owned()], now)
            .unwrap();

    assert_eq!(plan.key(), "asynq:{critical}:lease");
    assert_eq!(plan.task_ids(), &["task-id".to_owned()]);
    assert_eq!(
        plan.lease_expires_at(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_030)
    );
    assert_eq!(plan.lease_expires_at_seconds(), 1_700_000_030);

    let blank =
        RedisExtendLeasePlan::from_queue_and_task_ids("critical", &[String::new()], now).unwrap();
    assert_eq!(blank.key(), "asynq:{critical}:lease");
    assert_eq!(blank.task_ids(), &[String::new()]);
    assert_eq!(blank.lease_expires_at_seconds(), 1_700_000_030);

    let whitespace =
        RedisExtendLeasePlan::from_queue_and_task_ids("critical", &[" ".to_owned()], now).unwrap();
    assert_eq!(whitespace.task_ids(), &[" ".to_owned()]);
}

#[test]
fn plans_extend_lease_command_for_multiple_task_ids() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    let plan = RedisExtendLeasePlan::from_queue_and_task_ids(
        "critical",
        &["task-a".to_owned(), "task-b".to_owned()],
        now,
    )
    .unwrap();

    assert_eq!(plan.key(), "asynq:{critical}:lease");
    assert_eq!(plan.task_ids(), &["task-a".to_owned(), "task-b".to_owned()]);
    assert_eq!(
        plan.lease_expires_at(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_030)
    );
    assert_eq!(plan.lease_expires_at_seconds(), 1_700_000_030);
}

#[test]
fn plans_extend_lease_allows_blank_queue() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let plan =
        RedisExtendLeasePlan::from_queue_and_task_ids(" ", &["task-id".to_owned()], now).unwrap();

    assert_eq!(plan.key(), "asynq:{ }:lease");
    assert_eq!(plan.task_ids(), &["task-id".to_owned()]);
    assert_eq!(plan.lease_expires_at_seconds(), 1_700_000_030);
}
