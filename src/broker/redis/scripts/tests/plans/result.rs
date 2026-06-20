use super::*;

#[test]
fn redis_write_result_plan_has_no_script_shape() {
    let plan = RedisWriteResultPlan::from_task("critical", "task-id", b"result".to_vec()).unwrap();

    assert_eq!(plan.key(), "asynq:{critical}:t:task-id");
    assert_eq!(plan.field(), "result");
    assert_eq!(plan.data(), b"result");
}
