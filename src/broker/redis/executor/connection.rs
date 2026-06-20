use super::{RedisExecutor, RedisSlotRange, cluster::parse_cluster_slots};
use crate::broker::redis::{
    RedisArg, RedisDequeueCall, RedisExecutorError, RedisScript, RedisScriptCall,
};
use async_trait::async_trait;
use redis::AsyncCommands as _;

/// Async Redis executor backed by a redis-rs multiplexed connection.
#[derive(Debug, Clone)]
pub struct RedisConnectionExecutor<C> {
    connection: Option<C>,
}

impl<C> RedisConnectionExecutor<C> {
    pub fn new(connection: C) -> Self {
        Self {
            connection: Some(connection),
        }
    }

    pub fn connection(&self) -> &C {
        self.connection.as_ref().expect("redis connection is open")
    }

    pub fn connection_mut(&mut self) -> &mut C {
        self.connection.as_mut().expect("redis connection is open")
    }

    pub fn into_connection(mut self) -> Option<C> {
        self.connection.take()
    }

    fn connection_mut_result(&mut self) -> Result<&mut C, RedisExecutorError> {
        self.connection
            .as_mut()
            .ok_or_else(|| RedisExecutorError::new("connection closed"))
    }
}

#[async_trait]
impl<C> RedisExecutor for RedisConnectionExecutor<C>
where
    C: redis::aio::ConnectionLike + Send + Sync,
{
    fn close(&mut self) -> Result<(), RedisExecutorError> {
        self.connection.take();
        Ok(())
    }

    async fn ping(&mut self) -> Result<(), RedisExecutorError> {
        let pong: String = redis::cmd("PING")
            .query_async(self.connection_mut_result()?)
            .await
            .map_err(redis_executor_error)?;
        if pong == "PONG" {
            Ok(())
        } else {
            Err(RedisExecutorError::new(format!(
                "unexpected Redis PING response: {pong}"
            )))
        }
    }

    async fn sadd(&mut self, key: &str, member: &str) -> Result<(), RedisExecutorError> {
        let _: usize = self
            .connection_mut_result()?
            .sadd(key, member)
            .await
            .map_err(redis_executor_error)?;
        Ok(())
    }

    async fn smembers(&mut self, key: &str) -> Result<Vec<String>, RedisExecutorError> {
        let members: Vec<String> = self
            .connection_mut_result()?
            .smembers(key)
            .await
            .map_err(redis_executor_error)?;
        Ok(members)
    }

    async fn sismember(&mut self, key: &str, member: &str) -> Result<bool, RedisExecutorError> {
        self.connection_mut_result()?
            .sismember(key, member)
            .await
            .map_err(redis_executor_error)
    }

    async fn srem(&mut self, key: &str, member: &str) -> Result<usize, RedisExecutorError> {
        self.connection_mut_result()?
            .srem(key, member)
            .await
            .map_err(redis_executor_error)
    }

    async fn set_nx_i64(&mut self, key: &str, value: i64) -> Result<bool, RedisExecutorError> {
        self.connection_mut_result()?
            .set_nx(key, value)
            .await
            .map_err(redis_executor_error)
    }

    async fn zadd_existing(
        &mut self,
        key: &str,
        score: i64,
        member: &str,
    ) -> Result<usize, RedisExecutorError> {
        let options = redis::SortedSetAddOptions::update_only(None);
        self.connection_mut_result()?
            .zadd_options(key, member, score, &options)
            .await
            .map_err(redis_executor_error)
    }

    async fn zadd_existing_many(
        &mut self,
        key: &str,
        score: i64,
        members: &[String],
    ) -> Result<usize, RedisExecutorError> {
        let options = redis::SortedSetAddOptions::update_only(None);
        let items: Vec<(i64, &str)> = members
            .iter()
            .map(|member| (score, member.as_str()))
            .collect();
        self.connection_mut_result()?
            .zadd_multiple_options(key, &items, &options)
            .await
            .map_err(redis_executor_error)
    }

    async fn zadd(
        &mut self,
        key: &str,
        score: i64,
        member: &[u8],
    ) -> Result<usize, RedisExecutorError> {
        self.connection_mut_result()?
            .zadd(key, member, score)
            .await
            .map_err(redis_executor_error)
    }

    async fn lrange_bytes(
        &mut self,
        key: &str,
        start: usize,
        stop: isize,
    ) -> Result<Vec<Vec<u8>>, RedisExecutorError> {
        let values: Vec<Vec<u8>> = self
            .connection_mut_result()?
            .lrange(key, start as isize, stop)
            .await
            .map_err(redis_executor_error)?;
        Ok(values)
    }

    async fn zrevrange_bytes(
        &mut self,
        key: &str,
        start: isize,
        stop: isize,
    ) -> Result<Vec<Vec<u8>>, RedisExecutorError> {
        let values: Vec<Vec<u8>> = self
            .connection_mut_result()?
            .zrevrange(key, start, stop)
            .await
            .map_err(redis_executor_error)?;
        Ok(values)
    }

    async fn zrem(&mut self, key: &str, member: &str) -> Result<usize, RedisExecutorError> {
        self.connection_mut_result()?
            .zrem(key, member)
            .await
            .map_err(redis_executor_error)
    }

    async fn del(&mut self, key: &str) -> Result<usize, RedisExecutorError> {
        self.connection_mut_result()?
            .del(key)
            .await
            .map_err(redis_executor_error)
    }

    async fn get_bytes(&mut self, key: &str) -> Result<Option<Vec<u8>>, RedisExecutorError> {
        let value: Option<Vec<u8>> = self
            .connection_mut_result()?
            .get(key)
            .await
            .map_err(redis_executor_error)?;
        Ok(value)
    }

    async fn hget_bytes(
        &mut self,
        key: &str,
        field: &str,
    ) -> Result<Option<Vec<u8>>, RedisExecutorError> {
        let value: Option<Vec<u8>> = self
            .connection_mut_result()?
            .hget(key, field)
            .await
            .map_err(redis_executor_error)?;
        Ok(value)
    }

    async fn hvals_bytes(&mut self, key: &str) -> Result<Vec<Vec<u8>>, RedisExecutorError> {
        let values: Vec<Vec<u8>> = self
            .connection_mut_result()?
            .hvals(key)
            .await
            .map_err(redis_executor_error)?;
        Ok(values)
    }

    async fn eval_script_int(&mut self, call: &RedisScriptCall) -> Result<i64, RedisExecutorError> {
        eval_script_async(
            self.connection_mut_result()?,
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
            self.connection_mut_result()?,
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
            self.connection_mut_result()?,
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
            self.connection_mut_result()?,
            call.script(),
            call.keys(),
            call.args(),
        )
        .await
        .map_err(redis_executor_error)
    }

    async fn eval_script_value(
        &mut self,
        call: &RedisScriptCall,
    ) -> Result<redis::Value, RedisExecutorError> {
        eval_script_async(
            self.connection_mut_result()?,
            call.script(),
            call.keys(),
            call.args(),
        )
        .await
        .map_err(redis_executor_error)
    }

    async fn zscore(&mut self, key: &str, member: &str) -> Result<f64, RedisExecutorError> {
        self.connection_mut_result()?
            .zscore(key, member)
            .await
            .map_err(redis_executor_error)
    }

    async fn hset_bytes(
        &mut self,
        key: &str,
        field: &str,
        value: &[u8],
    ) -> Result<usize, RedisExecutorError> {
        self.connection_mut_result()?
            .hset(key, field, value)
            .await
            .map_err(redis_executor_error)
    }

    async fn publish(&mut self, channel: &str, payload: &str) -> Result<usize, RedisExecutorError> {
        self.connection_mut_result()?
            .publish(channel, payload)
            .await
            .map_err(redis_executor_error)
    }

    async fn cluster_key_slot(&mut self, key: &str) -> Result<i64, RedisExecutorError> {
        redis::cmd("CLUSTER")
            .arg("KEYSLOT")
            .arg(key)
            .query_async(self.connection_mut_result()?)
            .await
            .map_err(redis_executor_error)
    }

    async fn cluster_slots(&mut self) -> Result<Vec<RedisSlotRange>, RedisExecutorError> {
        let value = redis::cmd("CLUSTER")
            .arg("SLOTS")
            .query_async(self.connection_mut_result()?)
            .await
            .map_err(redis_executor_error)?;
        parse_cluster_slots(value)
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

#[cfg(test)]
mod tests {
    use futures_util::FutureExt as _;

    use super::*;

    #[derive(Debug)]
    struct TestConnection;

    impl redis::aio::ConnectionLike for TestConnection {
        fn req_packed_command<'a>(
            &'a mut self,
            _cmd: &'a redis::Cmd,
        ) -> redis::RedisFuture<'a, redis::Value> {
            async { Ok(redis::Value::Okay) }.boxed()
        }

        fn req_packed_commands<'a>(
            &'a mut self,
            _cmd: &'a redis::Pipeline,
            _offset: usize,
            _count: usize,
        ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
            async { Ok(Vec::new()) }.boxed()
        }

        fn get_db(&self) -> i64 {
            0
        }
    }

    #[tokio::test]
    async fn close_drops_connection_and_reports_later_commands_as_closed() {
        let mut executor = RedisConnectionExecutor::new(TestConnection);

        executor.close().unwrap();

        assert!(executor.into_connection().is_none());

        let mut executor = RedisConnectionExecutor::new(TestConnection);
        executor.close().unwrap();
        let error = executor.ping().await.unwrap_err();
        assert_eq!(error.message(), "connection closed");
    }
}
