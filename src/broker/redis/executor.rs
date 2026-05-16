use async_trait::async_trait;

use crate::{RedisArg, RedisDequeueCall, RedisExecutorError, RedisScript, RedisScriptCall};

/// Async executor surface for Redis-backed lifecycle operations.
///
/// Reference: Asynq v0.26.0 RDB methods use Redis commands and Lua scripts to
/// persist task lifecycle state:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L735>.
#[async_trait]
pub trait AsyncRedisExecutor {
    async fn sadd(&mut self, key: &str, member: &str) -> Result<(), RedisExecutorError>;

    async fn zadd_existing(
        &mut self,
        key: &str,
        score: i64,
        member: &str,
    ) -> Result<usize, RedisExecutorError>;

    async fn eval_script_int(&mut self, call: &RedisScriptCall) -> Result<i64, RedisExecutorError>;

    async fn eval_script_bytes(
        &mut self,
        call: &RedisDequeueCall,
    ) -> Result<Option<Vec<u8>>, RedisExecutorError>;

    async fn eval_script_byte_vec(
        &mut self,
        call: &RedisScriptCall,
    ) -> Result<Vec<Vec<u8>>, RedisExecutorError>;

    async fn eval_script_status(
        &mut self,
        call: &RedisScriptCall,
    ) -> Result<String, RedisExecutorError>;
}

/// Async Redis executor backed by a redis-rs multiplexed connection.
#[derive(Debug, Clone)]
pub struct AsyncRedisConnectionExecutor<C> {
    connection: C,
}

impl<C> AsyncRedisConnectionExecutor<C> {
    pub fn new(connection: C) -> Self {
        Self { connection }
    }

    pub fn connection(&self) -> &C {
        &self.connection
    }

    pub fn connection_mut(&mut self) -> &mut C {
        &mut self.connection
    }

    pub fn into_connection(self) -> C {
        self.connection
    }
}

#[async_trait]
impl<C> AsyncRedisExecutor for AsyncRedisConnectionExecutor<C>
where
    C: redis::aio::ConnectionLike + Send,
{
    async fn sadd(&mut self, key: &str, member: &str) -> Result<(), RedisExecutorError> {
        let _: usize = redis::cmd("SADD")
            .arg(key)
            .arg(member)
            .query_async(&mut self.connection)
            .await
            .map_err(redis_executor_error)?;
        Ok(())
    }

    async fn zadd_existing(
        &mut self,
        key: &str,
        score: i64,
        member: &str,
    ) -> Result<usize, RedisExecutorError> {
        redis::cmd("ZADD")
            .arg(key)
            .arg("XX")
            .arg(score)
            .arg(member)
            .query_async(&mut self.connection)
            .await
            .map_err(redis_executor_error)
    }

    async fn eval_script_int(&mut self, call: &RedisScriptCall) -> Result<i64, RedisExecutorError> {
        eval_script_async(
            &mut self.connection,
            call.script(),
            call.keys(),
            call.args(),
        )
        .await
        .map_err(redis_executor_error)
    }

    async fn eval_script_bytes(
        &mut self,
        call: &RedisDequeueCall,
    ) -> Result<Option<Vec<u8>>, RedisExecutorError> {
        eval_script_async(
            &mut self.connection,
            call.script(),
            call.keys(),
            call.args(),
        )
        .await
        .map_err(redis_executor_error)
    }

    async fn eval_script_byte_vec(
        &mut self,
        call: &RedisScriptCall,
    ) -> Result<Vec<Vec<u8>>, RedisExecutorError> {
        eval_script_async(
            &mut self.connection,
            call.script(),
            call.keys(),
            call.args(),
        )
        .await
        .map_err(redis_executor_error)
    }

    async fn eval_script_status(
        &mut self,
        call: &RedisScriptCall,
    ) -> Result<String, RedisExecutorError> {
        eval_script_async(
            &mut self.connection,
            call.script(),
            call.keys(),
            call.args(),
        )
        .await
        .map_err(redis_executor_error)
    }
}

async fn eval_script_async<T>(
    connection: &mut impl redis::aio::ConnectionLike,
    script: RedisScript,
    keys: &[String],
    args: &[RedisArg],
) -> Result<T, redis::RedisError>
where
    T: redis::FromRedisValue,
{
    let redis_script = redis::Script::new(script.source());
    let mut invocation = redis_script.prepare_invoke();
    for key in keys {
        invocation.key(key);
    }
    for arg in args {
        push_arg(&mut invocation, arg);
    }
    invocation.invoke_async(connection).await
}

fn push_arg(invocation: &mut redis::ScriptInvocation<'_>, arg: &RedisArg) {
    match arg {
        RedisArg::Bytes(value) => {
            invocation.arg(value.as_slice());
        }
        RedisArg::String(value) => {
            invocation.arg(value);
        }
        RedisArg::I64(value) => {
            invocation.arg(*value);
        }
    }
}

fn redis_executor_error(error: impl std::fmt::Display) -> RedisExecutorError {
    RedisExecutorError::new(error.to_string())
}
