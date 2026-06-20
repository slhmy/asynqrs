use super::*;

#[test]
fn redis_complete_plans_match_script_shapes() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut messages = Vec::new();
    messages.push(active_message(0, ""));
    messages.push(active_message(
        0,
        "asynq:{critical}:unique:email:welcome:321c3cf486ed509164edec1e1981fec8",
    ));
    messages.push(active_message(300, ""));
    messages.push(active_message(
        300,
        "asynq:{critical}:unique:email:welcome:321c3cf486ed509164edec1e1981fec8",
    ));

    for message in messages {
        let redis_plan = RedisCompletePlan::from_message(&message, now).unwrap();
        redis_plan.call().validate().unwrap();
    }
}

#[test]
fn redis_retry_plan_matches_script_shape() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let retry_at = now + Duration::from_secs(60);
    let message = active_message(0, "");

    let redis_plan =
        RedisRetryPlan::from_message(&message, now, retry_at, "handler failed", true).unwrap();

    redis_plan.call().validate().unwrap();
}

#[test]
fn redis_archive_plan_matches_script_shape() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let message = active_message(0, "");

    let redis_plan = RedisArchivePlan::from_message(&message, now, "max retry exhausted").unwrap();

    redis_plan.call().validate().unwrap();
}

#[test]
fn redis_requeue_plan_matches_script_shape() {
    let message = active_message(0, "");

    let redis_plan = RedisRequeuePlan::from_message(&message).unwrap();

    redis_plan.call().validate().unwrap();
}
