use super::*;
use crate::{EnqueueOptions, Task};

mod admin;
mod aggregation;
mod constants;
mod dequeue;
mod enqueue;
mod lease;
mod lifecycle;
mod maintenance;
mod metadata;
mod result;

fn too_large_go_duration() -> Duration {
    Duration::from_nanos(i64::MAX as u64) + Duration::from_nanos(1)
}

fn only_script(plan: &RedisEnqueuePlan) -> &RedisScriptCall {
    assert_eq!(plan.operations().len(), 2);
    match &plan.operations()[1] {
        RedisEnqueueOperation::EvalScript(call) => call,
        operation => panic!("expected script operation, got {operation:?}"),
    }
}

fn assert_encoded_message(arg: &RedisArg, expected: &TaskMessage) {
    let RedisArg::Bytes(data) = arg else {
        panic!("expected encoded message bytes, got {arg:?}");
    };
    let decoded = TaskMessage::decode_from_slice(data).unwrap();
    assert_eq!(&decoded, expected);
}

fn assert_completed_message(arg: &RedisArg, original: &TaskMessage, completed_at: i64) {
    let RedisArg::Bytes(data) = arg else {
        panic!("expected encoded message bytes, got {arg:?}");
    };
    let decoded = TaskMessage::decode_from_slice(data).unwrap();
    assert_eq!(decoded.completed_at, completed_at);
    let mut expected = original.clone();
    expected.completed_at = completed_at;
    assert_eq!(decoded, expected);
}

fn assert_retry_message(
    arg: &RedisArg,
    original: &TaskMessage,
    error_message: &str,
    last_failed_at: i64,
) {
    assert_retry_message_with_retried(
        arg,
        original,
        error_message,
        last_failed_at,
        original.retried + 1,
    );
}

fn assert_retry_message_with_retried(
    arg: &RedisArg,
    original: &TaskMessage,
    error_message: &str,
    last_failed_at: i64,
    retried: i32,
) {
    let RedisArg::Bytes(data) = arg else {
        panic!("expected encoded message bytes, got {arg:?}");
    };
    let decoded = TaskMessage::decode_from_slice(data).unwrap();
    let mut expected = original.clone();
    expected.retried = retried;
    expected.error_msg = error_message.to_owned();
    expected.last_failed_at = last_failed_at;
    assert_eq!(decoded, expected);
}

fn assert_failure_message(
    arg: &RedisArg,
    original: &TaskMessage,
    error_message: &str,
    last_failed_at: i64,
) {
    let RedisArg::Bytes(data) = arg else {
        panic!("expected encoded message bytes, got {arg:?}");
    };
    let decoded = TaskMessage::decode_from_slice(data).unwrap();
    let mut expected = original.clone();
    expected.error_msg = error_message.to_owned();
    expected.last_failed_at = last_failed_at;
    assert_eq!(decoded, expected);
}

fn active_message(retention: i64, unique_key: &str) -> TaskMessage {
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    msg.retention = retention;
    msg.unique_key = unique_key.to_owned();
    msg
}

fn plan_with(task: &Task, options: EnqueueOptions, now: SystemTime, task_id: &str) -> EnqueuePlan {
    EnqueuePlan::from_task_with_options(task, options, now, task_id).unwrap()
}
