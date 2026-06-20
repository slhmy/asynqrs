use crate::task::TaskMessage;

pub(in crate::broker::redis::broker::tests) fn redis_bulk(value: &str) -> redis::Value {
    redis::Value::BulkString(value.as_bytes().to_vec())
}

pub(in crate::broker::redis::broker::tests) fn redis_bytes(
    value: impl Into<Vec<u8>>,
) -> redis::Value {
    redis::Value::BulkString(value.into())
}

pub(in crate::broker::redis::broker::tests) fn task_info_value(
    message: &TaskMessage,
    state: &str,
    next_process_at: i64,
    result: Vec<u8>,
) -> redis::Value {
    redis::Value::Array(vec![
        redis_bytes(message.encode_to_vec()),
        redis_bulk(state),
        redis_bulk(&next_process_at.to_string()),
        if result.is_empty() {
            redis::Value::Nil
        } else {
            redis_bytes(result)
        },
    ])
}
