use super::*;

#[test]
fn plans_write_result_command() {
    let plan = RedisWriteResultPlan::from_task("critical", "task-id", b"result".to_vec()).unwrap();

    assert_eq!(plan.key(), "asynq:{critical}:t:task-id");
    assert_eq!(plan.field(), "result");
    assert_eq!(plan.data(), b"result");

    let blank = RedisWriteResultPlan::from_task("critical", "", b"result".to_vec()).unwrap();
    assert_eq!(blank.key(), "asynq:{critical}:t:");
    assert_eq!(blank.data(), b"result");

    let whitespace = RedisWriteResultPlan::from_task("critical", " ", b"result".to_vec()).unwrap();
    assert_eq!(whitespace.key(), "asynq:{critical}:t: ");
}

#[test]
fn plans_write_result_allows_blank_queue() {
    let plan = RedisWriteResultPlan::from_task(" ", "task-id", b"result".to_vec()).unwrap();

    assert_eq!(plan.key(), "asynq:{ }:t:task-id");
    assert_eq!(plan.field(), "result");
    assert_eq!(plan.data(), b"result");
}
