use crate::AdminError;

pub(in crate::broker::redis::broker) fn redis_value_to_string(
    value: &redis::Value,
) -> Result<String, AdminError> {
    match value {
        redis::Value::BulkString(bytes) => String::from_utf8(bytes.clone())
            .map_err(|error| AdminError::Other(format!("invalid UTF-8 Redis string: {error}"))),
        redis::Value::SimpleString(value) => Ok(value.clone()),
        redis::Value::Okay => Ok("OK".to_owned()),
        redis::Value::Int(value) => Ok(value.to_string()),
        value => Err(AdminError::Other(format!(
            "unexpected Redis string value: {value:?}"
        ))),
    }
}

pub(in crate::broker::redis::broker) fn redis_value_to_bytes(
    value: &redis::Value,
) -> Result<Vec<u8>, AdminError> {
    redis_value_to_optional_bytes(value)?
        .ok_or_else(|| AdminError::Other("unexpected nil Redis bytes".to_owned()))
}

pub(in crate::broker::redis::broker) fn redis_value_to_optional_bytes(
    value: &redis::Value,
) -> Result<Option<Vec<u8>>, AdminError> {
    match value {
        redis::Value::BulkString(bytes) => Ok(Some(bytes.clone())),
        redis::Value::SimpleString(value) => Ok(Some(value.as_bytes().to_vec())),
        redis::Value::Okay => Ok(Some(b"OK".to_vec())),
        redis::Value::Nil => Ok(None),
        value => Err(AdminError::Other(format!(
            "unexpected Redis bytes value: {value:?}"
        ))),
    }
}

pub(in crate::broker::redis::broker) fn redis_value_to_i64(
    value: &redis::Value,
) -> Result<i64, AdminError> {
    match value {
        redis::Value::Int(value) => Ok(*value),
        redis::Value::BulkString(bytes) => {
            let value = std::str::from_utf8(bytes).map_err(|error| {
                AdminError::Other(format!("invalid UTF-8 Redis integer: {error}"))
            })?;
            value.parse::<i64>().map_err(|error| {
                AdminError::Other(format!("invalid Redis integer {value:?}: {error}"))
            })
        }
        redis::Value::SimpleString(value) => value.parse::<i64>().map_err(|error| {
            AdminError::Other(format!("invalid Redis integer {value:?}: {error}"))
        }),
        redis::Value::Nil => Ok(0),
        value => Err(AdminError::Other(format!(
            "unexpected Redis integer value: {value:?}"
        ))),
    }
}

pub(in crate::broker::redis::broker) fn i64_to_usize(
    value: i64,
    context: &str,
) -> Result<usize, AdminError> {
    value
        .try_into()
        .map_err(|_| AdminError::Other(format!("{context} returned negative count: {value}")))
}
