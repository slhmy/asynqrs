use std::time::SystemTime;

use super::time::system_time_from_unix_seconds;
use super::value::{redis_value_to_bytes, redis_value_to_i64, redis_value_to_optional_bytes};
use crate::broker::redis::RedisBrokerError;
use crate::task::TaskMessage;
use crate::task::task_state_from_string;
use crate::{AdminError, TaskInfo, TaskState};

pub(in crate::broker::redis::broker) fn parse_task_infos(
    state: TaskState,
    values: Vec<Vec<u8>>,
    now: SystemTime,
) -> Result<Vec<TaskInfo>, AdminError> {
    if values.len() % 3 != 0 {
        return Err(AdminError::Other(format!(
            "list tasks script returned invalid tuple count: {}",
            values.len()
        )));
    }

    let mut infos = Vec::with_capacity(values.len() / 3);
    for chunk in values.chunks_exact(3) {
        // Reference: Asynq v0.26.0 `RDB.listZSetEntries` parses zset scores
        // before decoding messages, so malformed scores fail the listing while
        // malformed task payloads with valid scores are skipped.
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L844-L869>.
        let next_process_at = match state {
            TaskState::Pending => Some(now),
            TaskState::Scheduled | TaskState::Retry => Some(system_time_from_unix_seconds(
                parse_unix_seconds_bytes(&chunk[1])?,
            )),
            TaskState::Active
            | TaskState::Archived
            | TaskState::Completed
            | TaskState::Aggregating => None,
        };
        let Ok(message) = decode_admin_task_message(chunk[0].clone()) else {
            continue;
        };
        infos.push(TaskInfo::new(
            message,
            state,
            false,
            next_process_at,
            chunk[2].clone(),
        ));
    }
    // Reference: Asynq v0.26.0 reverses Redis list-backed task listings after
    // `LRANGE` so pending and active tasks are returned in processing order.
    // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L609-L610>.
    if matches!(state, TaskState::Pending | TaskState::Active) {
        infos.reverse();
    }
    Ok(infos)
}

pub(in crate::broker::redis::broker) fn parse_task_info_value(
    value: redis::Value,
) -> Result<TaskInfo, AdminError> {
    let values = match value {
        redis::Value::Array(values) => values,
        value => {
            return Err(AdminError::Other(format!(
                "unexpected task info script result: {value:?}"
            )));
        }
    };
    if values.len() != 4 {
        return Err(AdminError::Other(format!(
            "task info script returned unexpected value count: {}",
            values.len()
        )));
    }

    let message = decode_admin_task_message(redis_value_to_bytes(&values[0])?)?;
    let state = parse_task_state_bytes(redis_value_to_bytes(&values[1])?)?;
    let next_process_at_unix = redis_value_to_i64(&values[2])?;
    let next_process_at =
        (next_process_at_unix != 0).then(|| system_time_from_unix_seconds(next_process_at_unix));
    let result = redis_value_to_optional_bytes(&values[3])?.unwrap_or_default();

    Ok(TaskInfo::new(
        message,
        state,
        false,
        next_process_at,
        result,
    ))
}

pub(in crate::broker::redis::broker) fn is_not_found_script_error(message: &str) -> bool {
    message.contains("NOT FOUND") || (message.contains("NOT") && message.contains("FOUND"))
}

fn decode_admin_task_message(data: Vec<u8>) -> Result<TaskMessage, AdminError> {
    TaskMessage::decode_from_slice(&data)
        .map_err(RedisBrokerError::Decode)
        .map_err(AdminError::from)
}

fn parse_task_state_bytes(data: Vec<u8>) -> Result<TaskState, AdminError> {
    let value = String::from_utf8(data)
        .map_err(|error| AdminError::Other(format!("invalid UTF-8 task state: {error}")))?;
    task_state_from_string(&value).map_err(|error| AdminError::Other(error.to_string()))
}

fn parse_unix_seconds_bytes(data: &[u8]) -> Result<i64, AdminError> {
    let value = std::str::from_utf8(data)
        .map_err(|error| AdminError::Other(format!("invalid UTF-8 unix timestamp: {error}")))?;
    value
        .parse::<i64>()
        .map_err(|error| AdminError::Other(format!("invalid unix timestamp: {error}")))
}
