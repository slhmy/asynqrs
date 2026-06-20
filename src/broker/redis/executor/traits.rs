use async_trait::async_trait;

use super::RedisSlotRange;
use crate::broker::redis::{RedisDequeueCall, RedisExecutorError, RedisScriptCall};

/// Async executor surface for Redis-backed lifecycle operations.
///
/// Reference: Asynq v0.26.0 RDB methods use Redis commands and Lua scripts to
/// persist task lifecycle state:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L735>.
#[async_trait]
pub trait RedisExecutor {
    /// Closes executor-owned Redis resources.
    ///
    /// Reference: Asynq v0.26.0 `RDB.Close` delegates to the underlying Redis
    /// client close method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L109-L111>.
    fn close(&mut self) -> Result<(), RedisExecutorError> {
        Ok(())
    }

    async fn ping(&mut self) -> Result<(), RedisExecutorError>;

    async fn sadd(&mut self, key: &str, member: &str) -> Result<(), RedisExecutorError>;

    async fn smembers(&mut self, key: &str) -> Result<Vec<String>, RedisExecutorError>;

    async fn sismember(&mut self, key: &str, member: &str) -> Result<bool, RedisExecutorError>;

    async fn srem(&mut self, key: &str, member: &str) -> Result<usize, RedisExecutorError>;

    async fn set_nx_i64(&mut self, key: &str, value: i64) -> Result<bool, RedisExecutorError>;

    async fn zadd_existing(
        &mut self,
        key: &str,
        score: i64,
        member: &str,
    ) -> Result<usize, RedisExecutorError>;

    async fn zadd_existing_many(
        &mut self,
        key: &str,
        score: i64,
        members: &[String],
    ) -> Result<usize, RedisExecutorError>;

    async fn zadd(
        &mut self,
        key: &str,
        score: i64,
        member: &[u8],
    ) -> Result<usize, RedisExecutorError>;

    async fn lrange_bytes(
        &mut self,
        key: &str,
        start: usize,
        stop: isize,
    ) -> Result<Vec<Vec<u8>>, RedisExecutorError>;

    async fn zrevrange_bytes(
        &mut self,
        key: &str,
        start: isize,
        stop: isize,
    ) -> Result<Vec<Vec<u8>>, RedisExecutorError>;

    async fn zrem(&mut self, key: &str, member: &str) -> Result<usize, RedisExecutorError>;

    async fn del(&mut self, key: &str) -> Result<usize, RedisExecutorError>;

    async fn get_bytes(&mut self, key: &str) -> Result<Option<Vec<u8>>, RedisExecutorError>;

    async fn hget_bytes(
        &mut self,
        key: &str,
        field: &str,
    ) -> Result<Option<Vec<u8>>, RedisExecutorError>;

    async fn hvals_bytes(&mut self, key: &str) -> Result<Vec<Vec<u8>>, RedisExecutorError>;

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

    async fn eval_script_value(
        &mut self,
        call: &RedisScriptCall,
    ) -> Result<redis::Value, RedisExecutorError>;

    async fn zscore(&mut self, key: &str, member: &str) -> Result<f64, RedisExecutorError>;

    async fn hset_bytes(
        &mut self,
        key: &str,
        field: &str,
        value: &[u8],
    ) -> Result<usize, RedisExecutorError>;

    async fn publish(&mut self, channel: &str, payload: &str) -> Result<usize, RedisExecutorError>;

    async fn cluster_key_slot(&mut self, key: &str) -> Result<i64, RedisExecutorError>;

    async fn cluster_slots(&mut self) -> Result<Vec<RedisSlotRange>, RedisExecutorError>;
}
