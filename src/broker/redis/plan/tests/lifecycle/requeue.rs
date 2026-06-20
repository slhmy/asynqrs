use super::*;

#[test]
fn plans_requeue_script_for_active_task() {
    let msg = active_message(0, "");

    let plan = RedisRequeuePlan::from_message(&msg).unwrap();
    let call = plan.call();

    assert_eq!(call.script(), RedisScript::Requeue);
    assert_eq!(
        call.keys(),
        &[
            "asynq:{critical}:active".to_owned(),
            "asynq:{critical}:lease".to_owned(),
            "asynq:{critical}:pending".to_owned(),
            "asynq:{critical}:t:task-id".to_owned(),
        ]
    );
    assert_eq!(call.args(), &[RedisArg::String("task-id".to_owned())]);

    let mut blank_id = active_message(0, "");
    blank_id.id = String::new();
    let blank_plan = RedisRequeuePlan::from_message(&blank_id).unwrap();
    assert_eq!(
        blank_plan.call().keys()[3],
        "asynq:{critical}:t:".to_owned()
    );
    assert_eq!(blank_plan.call().args(), &[RedisArg::String(String::new())]);
}

#[test]
fn plans_requeue_allows_blank_queue_and_task_id() {
    let mut msg = active_message(0, "");

    msg.queue = " ".to_owned();
    let blank_queue = RedisRequeuePlan::from_message(&msg).unwrap();
    assert_eq!(
        blank_queue.call().keys(),
        &[
            "asynq:{ }:active".to_owned(),
            "asynq:{ }:lease".to_owned(),
            "asynq:{ }:pending".to_owned(),
            "asynq:{ }:t:task-id".to_owned(),
        ]
    );

    msg.queue = "critical".to_owned();
    msg.id = " ".to_owned();
    assert!(RedisRequeuePlan::from_message(&msg).is_ok());
}
