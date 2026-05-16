use crate::{BrokerError, RedisScriptCall, RedisScriptResult, SystemClock};

mod r#async;
mod error;

pub use error::{RedisBrokerError, RedisExecutorError};

#[derive(Debug, Clone)]
pub struct AsyncRedisBroker<E, C = SystemClock> {
    executor: E,
    clock: C,
}

impl<E> AsyncRedisBroker<E, SystemClock> {
    pub fn new(executor: E) -> Self {
        Self::with_clock(executor, SystemClock)
    }
}

impl<E, C> AsyncRedisBroker<E, C> {
    pub fn with_clock(executor: E, clock: C) -> Self {
        Self { executor, clock }
    }

    pub fn executor(&self) -> &E {
        &self.executor
    }

    pub fn executor_mut(&mut self) -> &mut E {
        &mut self.executor
    }

    pub fn into_executor(self) -> E {
        self.executor
    }
}

fn map_script_result(call: &RedisScriptCall, result: i64) -> Result<(), BrokerError> {
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

#[cfg(test)]
mod tests;
