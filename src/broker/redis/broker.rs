use crate::{BrokerError, RedisDequeueCall, RedisScriptCall, RedisScriptResult, SystemClock};

mod r#async;
mod error;
mod sync;

pub use error::{RedisBrokerError, RedisExecutorError};

/// Minimal executor surface needed by `RedisBroker`.
///
/// Reference: Asynq v0.26.0 RDB methods combine Redis commands with Lua
/// scripts for task lifecycle state changes:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L735>.
pub trait RedisExecutor {
    fn sadd(&mut self, key: &str, member: &str) -> Result<(), RedisExecutorError>;

    fn zadd_existing(
        &mut self,
        key: &str,
        score: i64,
        member: &str,
    ) -> Result<usize, RedisExecutorError>;

    fn eval_script_int(&mut self, call: &RedisScriptCall) -> Result<i64, RedisExecutorError>;

    fn eval_script_bytes(
        &mut self,
        call: &RedisDequeueCall,
    ) -> Result<Option<Vec<u8>>, RedisExecutorError>;

    fn eval_script_byte_vec(
        &mut self,
        call: &RedisScriptCall,
    ) -> Result<Vec<Vec<u8>>, RedisExecutorError>;

    fn eval_script_status(&mut self, call: &RedisScriptCall) -> Result<String, RedisExecutorError>;
}

#[derive(Debug, Clone)]
pub struct AsyncRedisBroker<E, C = SystemClock> {
    executor: E,
    clock: C,
}

#[derive(Debug, Clone)]
pub struct RedisBroker<E, C = SystemClock> {
    executor: E,
    clock: C,
}

impl<E> RedisBroker<E, SystemClock> {
    pub fn new(executor: E) -> Self {
        Self::with_clock(executor, SystemClock)
    }
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

impl<E, C> RedisBroker<E, C> {
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
