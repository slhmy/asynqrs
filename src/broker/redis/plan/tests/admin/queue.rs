use super::*;

#[test]
fn plans_admin_queue_commands() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    let list = RedisListQueuesPlan::new();
    assert_eq!(list.key(), "asynq:queues");

    let pause = RedisPauseQueuePlan::from_queue("critical", now).unwrap();
    assert_eq!(pause.paused_key(), "asynq:{critical}:paused");
    assert_eq!(pause.paused_at(), 1_700_000_000);

    let unpause = RedisUnpauseQueuePlan::from_queue("critical").unwrap();
    assert_eq!(unpause.paused_key(), "asynq:{critical}:paused");

    let delete = RedisDeleteQueuePlan::from_queue("critical").unwrap();
    let call = delete.call();
    assert_eq!(delete.queue(), "critical");
    assert_eq!(call.script(), RedisScript::DeleteQueue);
    assert_eq!(delete.queue_set_key(), "asynq:queues");
    assert_eq!(
        call.keys(),
        &[
            "asynq:{critical}:pending".to_owned(),
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:scheduled".to_owned(),
            "asynq:{critical}:retry".to_owned(),
            "asynq:{critical}:archived".to_owned(),
            "asynq:{critical}:lease".to_owned(),
        ]
    );
    assert_eq!(
        call.args(),
        &[RedisArg::String("asynq:{critical}:t:".to_owned())]
    );

    let delete_force = RedisDeleteQueuePlan::from_queue_and_force("critical", true).unwrap();
    assert_eq!(delete_force.call().script(), RedisScript::DeleteQueueForce);
    assert_eq!(
        delete_force.call().args(),
        &[RedisArg::String("asynq:{critical}:t:".to_owned())]
    );

    let blank_delete = RedisDeleteQueuePlan::from_queue(" ").unwrap();
    assert_eq!(blank_delete.queue(), " ");
    assert_eq!(blank_delete.call().script(), RedisScript::DeleteQueue);
    assert_eq!(
        blank_delete.call().keys(),
        &[
            "asynq:{ }:pending".to_owned(),
            "asynq:{ }:active".to_owned(),
            "asynq:{ }:scheduled".to_owned(),
            "asynq:{ }:retry".to_owned(),
            "asynq:{ }:archived".to_owned(),
            "asynq:{ }:lease".to_owned(),
        ]
    );
    assert_eq!(
        blank_delete.call().args(),
        &[RedisArg::String("asynq:{ }:t:".to_owned())]
    );

    let blank_delete_force = RedisDeleteQueuePlan::from_queue_and_force(" ", true).unwrap();
    assert_eq!(blank_delete_force.queue(), " ");
    assert_eq!(
        blank_delete_force.call().script(),
        RedisScript::DeleteQueueForce
    );
    assert_eq!(
        blank_delete_force.call().args(),
        &[RedisArg::String("asynq:{ }:t:".to_owned())]
    );
}
