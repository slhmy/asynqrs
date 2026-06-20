use super::*;

#[test]
fn plans_dequeue_calls_for_queues() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let queues = vec!["critical".to_owned(), "default".to_owned()];

    let plan = RedisDequeuePlan::from_queues(&queues, now).unwrap();

    assert_eq!(
        plan.lease_expires_at(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_030)
    );
    assert_eq!(plan.queue_calls().len(), 2);
    let call = &plan.queue_calls()[0];
    assert_eq!(call.queue(), "critical");
    assert_eq!(call.script(), RedisScript::Dequeue);
    assert_eq!(
        call.keys(),
        &[
            "asynq:{critical}:pending".to_owned(),
            "asynq:{critical}:paused".to_owned(),
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:lease".to_owned(),
        ]
    );
    assert_eq!(
        call.args(),
        &[
            RedisArg::I64(1_700_000_030),
            RedisArg::String("asynq:{critical}:t:".to_owned()),
        ]
    );
}

#[test]
fn plans_dequeue_allows_empty_queue_list() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let plan = RedisDequeuePlan::from_queues(&[], now).unwrap();

    assert!(plan.queue_calls().is_empty());
    assert_eq!(
        plan.lease_expires_at(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_030)
    );
}

#[test]
fn plans_dequeue_allows_blank_queue() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let plan = RedisDequeuePlan::from_queues(&[" ".to_owned()], now).unwrap();

    assert_eq!(plan.queue_calls().len(), 1);
    assert_eq!(plan.queue_calls()[0].queue(), " ");
    assert_eq!(
        plan.queue_calls()[0].keys(),
        &[
            "asynq:{ }:pending".to_owned(),
            "asynq:{ }:paused".to_owned(),
            "asynq:{ }:active".to_owned(),
            "asynq:{ }:lease".to_owned(),
        ]
    );
    assert_eq!(
        plan.queue_calls()[0].args(),
        &[
            RedisArg::I64(1_700_000_030),
            RedisArg::String("asynq:{ }:t:".to_owned()),
        ]
    );
}
