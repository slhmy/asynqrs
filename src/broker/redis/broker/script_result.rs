use crate::BrokerError;
use crate::broker::redis::{RedisScriptCall, RedisScriptResult};

use super::RedisBrokerError;

pub(in crate::broker::redis::broker) fn map_script_result(
    call: &RedisScriptCall,
    result: i64,
) -> Result<(), BrokerError> {
    match call.script().result_for_code(result) {
        Some(RedisScriptResult::Success) => Ok(()),
        Some(RedisScriptResult::TaskIdConflict) => Err(BrokerError::TaskIdConflict),
        Some(RedisScriptResult::DuplicateTask) => Err(BrokerError::DuplicateTask),
        None => Err(BrokerError::from(
            RedisBrokerError::UnexpectedScriptResult {
                script: call.script(),
                result,
            },
        )),
    }
}
