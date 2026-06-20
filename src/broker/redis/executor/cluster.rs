use super::RedisSlotRange;
use crate::ClusterNode;
use crate::broker::redis::RedisExecutorError;

pub(super) fn parse_cluster_slots(
    value: redis::Value,
) -> Result<Vec<RedisSlotRange>, RedisExecutorError> {
    let redis::Value::Array(slots) = value else {
        return Err(RedisExecutorError::new(format!(
            "invalid CLUSTER SLOTS response: {value:?}"
        )));
    };
    let mut ranges = Vec::new();
    for slot in slots {
        let redis::Value::Array(items) = slot else {
            continue;
        };
        if items.len() < 3 {
            continue;
        }
        let Some(start) = redis_value_i64(&items[0]) else {
            continue;
        };
        let Some(end) = redis_value_i64(&items[1]) else {
            continue;
        };
        let nodes = items
            .iter()
            .skip(2)
            .filter_map(redis_cluster_node)
            .collect::<Vec<_>>();
        if nodes.is_empty() {
            continue;
        }
        ranges.push(RedisSlotRange::new(start, end, nodes));
    }
    Ok(ranges)
}

fn redis_cluster_node(value: &redis::Value) -> Option<ClusterNode> {
    let redis::Value::Array(items) = value else {
        return None;
    };
    let host = redis_value_string(items.first()?)?;
    if host.is_empty() || host == "?" {
        return None;
    }
    let port = redis_value_i64(items.get(1)?)?;
    let id = items
        .get(2)
        .and_then(redis_value_string)
        .unwrap_or_default();
    Some(ClusterNode::new(id, format!("{host}:{port}")))
}

fn redis_value_i64(value: &redis::Value) -> Option<i64> {
    match value {
        redis::Value::Int(value) => Some(*value),
        redis::Value::BulkString(value) => std::str::from_utf8(value).ok()?.parse().ok(),
        redis::Value::SimpleString(value) => value.parse().ok(),
        _ => None,
    }
}

fn redis_value_string(value: &redis::Value) -> Option<String> {
    match value {
        redis::Value::BulkString(value) => String::from_utf8(value.clone()).ok(),
        redis::Value::SimpleString(value) => Some(value.clone()),
        _ => None,
    }
}
