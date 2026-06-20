use super::*;

#[test]
fn redis_forward_plans_match_script_shapes() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    RedisForwardPlan::from_scheduled_queue("critical", now)
        .unwrap()
        .call()
        .validate()
        .unwrap();
    RedisForwardPlan::from_retry_queue("critical", now)
        .unwrap()
        .call()
        .validate()
        .unwrap();
}

#[test]
fn redis_delete_expired_completed_tasks_plan_matches_script_shape() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    RedisDeleteExpiredCompletedTasksPlan::from_queue("critical", now, 100)
        .unwrap()
        .call()
        .validate()
        .unwrap();
}

#[test]
fn redis_recover_plan_matches_script_shape() {
    let cutoff = UNIX_EPOCH + Duration::from_secs(1_699_999_970);

    RedisRecoverPlan::from_queue_and_cutoff("critical", cutoff)
        .unwrap()
        .call()
        .validate()
        .unwrap();
}

#[test]
fn redis_extend_lease_plan_has_no_script_shape() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    let plan =
        RedisExtendLeasePlan::from_queue_and_task_ids("critical", &["task-id".to_owned()], now)
            .unwrap();

    assert_eq!(plan.key(), "asynq:{critical}:lease");
    assert_eq!(plan.task_ids(), &["task-id".to_owned()]);
    assert_eq!(plan.lease_expires_at_seconds(), 1_700_000_030);
}
