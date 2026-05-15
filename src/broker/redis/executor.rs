use async_trait::async_trait;

use crate::{
    RedisArg, RedisDequeueCall, RedisExecutor, RedisExecutorError, RedisScript, RedisScriptCall,
};

/// Provides a Redis connection for command execution.
///
/// This is a Rust-specific adapter boundary for client or pool types. The
/// task lifecycle commands it feeds are still the upstream Asynq Redis
/// operations.
///
/// Reference: Asynq v0.26.0 RDB methods use Redis commands and Lua scripts to
/// persist task lifecycle state:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L735>.
pub trait RedisConnectionProvider {
    type Connection: RedisCommandExecutor;
    type Error: std::fmt::Display;

    fn get_connection(&mut self) -> Result<Self::Connection, Self::Error>;
}

/// Redis executor backed by a connection provider, such as `redis::Client`.
#[derive(Debug, Clone)]
pub struct RedisConnectionProviderExecutor<P> {
    provider: P,
}

/// Convenience executor type for a synchronous redis-rs `Client`.
pub type RedisClientExecutor = RedisConnectionProviderExecutor<redis::Client>;

/// Synchronous Redis executor backed by a redis-rs connection-like type.
///
/// Reference: Asynq v0.26.0 RDB methods use Redis commands and Lua scripts to
/// persist task lifecycle state:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L735>.
#[derive(Debug, Clone)]
pub struct RedisConnectionExecutor<C> {
    connection: C,
}

pub trait RedisCommandExecutor {
    type Error: std::fmt::Display;

    fn sadd(&mut self, key: &str, member: &str) -> Result<(), Self::Error>;

    fn zadd_existing(&mut self, key: &str, score: i64, member: &str) -> Result<usize, Self::Error>;

    fn eval_script_int(&mut self, call: &RedisScriptCall) -> Result<i64, Self::Error>;

    fn eval_script_bytes(
        &mut self,
        call: &RedisDequeueCall,
    ) -> Result<Option<Vec<u8>>, Self::Error>;

    fn eval_script_byte_vec(&mut self, call: &RedisScriptCall)
    -> Result<Vec<Vec<u8>>, Self::Error>;

    fn eval_script_status(&mut self, call: &RedisScriptCall) -> Result<String, Self::Error>;
}

/// Async executor surface for Redis-backed lifecycle operations.
///
/// Reference: Asynq v0.26.0 RDB methods use Redis commands and Lua scripts to
/// persist task lifecycle state:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L735>.
///
/// TODO: Extend async broker coverage beyond enqueue once async lifecycle
/// traits for dequeue, completion, retry, archive, recovery, and lease
/// extension are introduced.
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

impl<P> RedisConnectionProviderExecutor<P> {
    pub fn new(provider: P) -> Self {
        Self { provider }
    }

    pub fn provider(&self) -> &P {
        &self.provider
    }

    pub fn provider_mut(&mut self) -> &mut P {
        &mut self.provider
    }

    pub fn into_provider(self) -> P {
        self.provider
    }
}

impl<C> RedisConnectionExecutor<C> {
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

impl<P> RedisExecutor for RedisConnectionProviderExecutor<P>
where
    P: RedisConnectionProvider,
{
    fn sadd(&mut self, key: &str, member: &str) -> Result<(), RedisExecutorError> {
        self.with_connection(|connection| connection.sadd(key, member))
    }

    fn zadd_existing(
        &mut self,
        key: &str,
        score: i64,
        member: &str,
    ) -> Result<usize, RedisExecutorError> {
        self.with_connection(|connection| connection.zadd_existing(key, score, member))
    }

    fn eval_script_int(&mut self, call: &RedisScriptCall) -> Result<i64, RedisExecutorError> {
        self.with_connection(|connection| connection.eval_script_int(call))
    }

    fn eval_script_bytes(
        &mut self,
        call: &RedisDequeueCall,
    ) -> Result<Option<Vec<u8>>, RedisExecutorError> {
        self.with_connection(|connection| connection.eval_script_bytes(call))
    }

    fn eval_script_byte_vec(
        &mut self,
        call: &RedisScriptCall,
    ) -> Result<Vec<Vec<u8>>, RedisExecutorError> {
        self.with_connection(|connection| connection.eval_script_byte_vec(call))
    }

    fn eval_script_status(&mut self, call: &RedisScriptCall) -> Result<String, RedisExecutorError> {
        self.with_connection(|connection| connection.eval_script_status(call))
    }
}

impl<P> RedisConnectionProviderExecutor<P>
where
    P: RedisConnectionProvider,
{
    fn with_connection<R>(
        &mut self,
        command: impl FnOnce(
            &mut P::Connection,
        ) -> Result<R, <P::Connection as RedisCommandExecutor>::Error>,
    ) -> Result<R, RedisExecutorError> {
        let mut connection = self
            .provider
            .get_connection()
            .map_err(redis_executor_error)?;
        command(&mut connection).map_err(redis_executor_error)
    }
}

impl<C> RedisExecutor for RedisConnectionExecutor<C>
where
    C: RedisCommandExecutor,
{
    fn sadd(&mut self, key: &str, member: &str) -> Result<(), RedisExecutorError> {
        self.connection
            .sadd(key, member)
            .map_err(redis_executor_error)
    }

    fn zadd_existing(
        &mut self,
        key: &str,
        score: i64,
        member: &str,
    ) -> Result<usize, RedisExecutorError> {
        self.connection
            .zadd_existing(key, score, member)
            .map_err(redis_executor_error)
    }

    fn eval_script_int(&mut self, call: &RedisScriptCall) -> Result<i64, RedisExecutorError> {
        self.connection
            .eval_script_int(call)
            .map_err(redis_executor_error)
    }

    fn eval_script_bytes(
        &mut self,
        call: &RedisDequeueCall,
    ) -> Result<Option<Vec<u8>>, RedisExecutorError> {
        self.connection
            .eval_script_bytes(call)
            .map_err(redis_executor_error)
    }

    fn eval_script_byte_vec(
        &mut self,
        call: &RedisScriptCall,
    ) -> Result<Vec<Vec<u8>>, RedisExecutorError> {
        self.connection
            .eval_script_byte_vec(call)
            .map_err(redis_executor_error)
    }

    fn eval_script_status(&mut self, call: &RedisScriptCall) -> Result<String, RedisExecutorError> {
        self.connection
            .eval_script_status(call)
            .map_err(redis_executor_error)
    }
}

impl RedisConnectionProvider for redis::Client {
    type Connection = redis::Connection;
    type Error = redis::RedisError;

    fn get_connection(&mut self) -> Result<Self::Connection, Self::Error> {
        redis::Client::get_connection(self)
    }
}

impl<T> RedisCommandExecutor for T
where
    T: redis::ConnectionLike,
{
    type Error = redis::RedisError;

    fn sadd(&mut self, key: &str, member: &str) -> Result<(), Self::Error> {
        let _: usize = redis::cmd("SADD").arg(key).arg(member).query(self)?;
        Ok(())
    }

    fn zadd_existing(&mut self, key: &str, score: i64, member: &str) -> Result<usize, Self::Error> {
        redis::cmd("ZADD")
            .arg(key)
            .arg("XX")
            .arg(score)
            .arg(member)
            .query(self)
    }

    fn eval_script_int(&mut self, call: &RedisScriptCall) -> Result<i64, Self::Error> {
        eval_script(self, call.script(), call.keys(), call.args())
    }

    fn eval_script_bytes(
        &mut self,
        call: &RedisDequeueCall,
    ) -> Result<Option<Vec<u8>>, Self::Error> {
        eval_script(self, call.script(), call.keys(), call.args())
    }

    fn eval_script_byte_vec(
        &mut self,
        call: &RedisScriptCall,
    ) -> Result<Vec<Vec<u8>>, Self::Error> {
        eval_script(self, call.script(), call.keys(), call.args())
    }

    fn eval_script_status(&mut self, call: &RedisScriptCall) -> Result<String, Self::Error> {
        eval_script(self, call.script(), call.keys(), call.args())
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

fn eval_script<T>(
    connection: &mut impl redis::ConnectionLike,
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
    invocation.invoke(connection)
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
    use super::*;
    use crate::RedisDequeuePlan;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::time::{Duration, UNIX_EPOCH};

    #[derive(Debug, Default)]
    struct FakeConnection {
        sadd_calls: Vec<(String, String)>,
        script_int_calls: Vec<(RedisScript, Vec<String>, Vec<RedisArg>)>,
        script_bytes_calls: Vec<(RedisScript, Vec<String>, Vec<RedisArg>)>,
        script_status_calls: Vec<(RedisScript, Vec<String>, Vec<RedisArg>)>,
        script_int_results: Vec<i64>,
        script_bytes_results: Vec<Option<Vec<u8>>>,
        script_status_results: Vec<String>,
        sadd_error: Option<String>,
        script_error: Option<String>,
    }

    #[derive(Debug, Clone)]
    struct FakeError(String);

    impl RedisCommandExecutor for FakeConnection {
        type Error = FakeError;

        fn sadd(&mut self, key: &str, member: &str) -> Result<(), Self::Error> {
            self.sadd_calls.push((key.to_owned(), member.to_owned()));
            if let Some(error) = &self.sadd_error {
                return Err(FakeError(error.clone()));
            }
            Ok(())
        }

        fn zadd_existing(
            &mut self,
            key: &str,
            score: i64,
            member: &str,
        ) -> Result<usize, Self::Error> {
            self.script_int_calls.push((
                RedisScript::ListLeaseExpired,
                vec![key.to_owned(), member.to_owned()],
                vec![RedisArg::I64(score)],
            ));
            if let Some(error) = &self.script_error {
                return Err(FakeError(error.clone()));
            }
            Ok(1)
        }

        fn eval_script_int(&mut self, call: &RedisScriptCall) -> Result<i64, Self::Error> {
            self.script_int_calls
                .push((call.script(), call.keys().to_vec(), call.args().to_vec()));
            if let Some(error) = &self.script_error {
                return Err(FakeError(error.clone()));
            }
            Ok(self.script_int_results.pop().unwrap_or(1))
        }

        fn eval_script_bytes(
            &mut self,
            call: &RedisDequeueCall,
        ) -> Result<Option<Vec<u8>>, Self::Error> {
            self.script_bytes_calls.push((
                call.script(),
                call.keys().to_vec(),
                call.args().to_vec(),
            ));
            if let Some(error) = &self.script_error {
                return Err(FakeError(error.clone()));
            }
            Ok(self.script_bytes_results.pop().unwrap_or(None))
        }

        fn eval_script_byte_vec(
            &mut self,
            call: &RedisScriptCall,
        ) -> Result<Vec<Vec<u8>>, Self::Error> {
            self.script_int_calls
                .push((call.script(), call.keys().to_vec(), call.args().to_vec()));
            if let Some(error) = &self.script_error {
                return Err(FakeError(error.clone()));
            }
            Ok(self
                .script_bytes_results
                .pop()
                .flatten()
                .map(|data| vec![data])
                .unwrap_or_default())
        }

        fn eval_script_status(&mut self, call: &RedisScriptCall) -> Result<String, Self::Error> {
            self.script_status_calls.push((
                call.script(),
                call.keys().to_vec(),
                call.args().to_vec(),
            ));
            if let Some(error) = &self.script_error {
                return Err(FakeError(error.clone()));
            }
            Ok(self
                .script_status_results
                .pop()
                .unwrap_or_else(|| "OK".to_owned()))
        }
    }

    impl std::fmt::Display for FakeError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(&self.0)
        }
    }

    #[test]
    fn forwards_sadd_to_connection() {
        let mut executor = RedisConnectionExecutor::new(FakeConnection::default());

        executor.sadd("asynq:queues", "critical").unwrap();

        assert_eq!(
            executor.connection().sadd_calls,
            [("asynq:queues".to_owned(), "critical".to_owned())]
        );
    }

    #[test]
    fn forwards_integer_script_eval_to_connection() {
        let mut executor = RedisConnectionExecutor::new(FakeConnection {
            script_int_results: vec![1],
            ..FakeConnection::default()
        });
        let keys = vec![
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:pending".to_owned(),
        ];
        let args = vec![
            RedisArg::Bytes(b"message".to_vec()),
            RedisArg::String("task-id".to_owned()),
            RedisArg::I64(1_700_000_000),
        ];
        let call = RedisScriptCall::new(RedisScript::Enqueue, keys, args);

        let result = executor.eval_script_int(&call).unwrap();

        assert_eq!(result, 1);
        assert_eq!(
            executor.connection().script_int_calls,
            [(
                RedisScript::Enqueue,
                call.keys().to_vec(),
                call.args().to_vec()
            )]
        );
    }

    #[test]
    fn forwards_byte_script_eval_to_connection() {
        let mut executor = RedisConnectionExecutor::new(FakeConnection {
            script_bytes_results: vec![Some(b"message".to_vec())],
            ..FakeConnection::default()
        });
        let call = dequeue_call();

        let result = executor.eval_script_bytes(&call).unwrap();

        assert_eq!(result, Some(b"message".to_vec()));
        assert_eq!(
            executor.connection().script_bytes_calls,
            [(
                RedisScript::Dequeue,
                call.keys().to_vec(),
                call.args().to_vec()
            )]
        );
    }

    #[test]
    fn forwards_status_script_eval_to_connection() {
        let mut executor = RedisConnectionExecutor::new(FakeConnection {
            script_status_results: vec!["OK".to_owned()],
            ..FakeConnection::default()
        });
        let call = RedisScriptCall::new(
            RedisScript::Done,
            vec![
                "asynq:{critical}:active".to_owned(),
                "asynq:{critical}:lease".to_owned(),
                "asynq:{critical}:t:task-id".to_owned(),
                "asynq:{critical}:processed:2023-11-14".to_owned(),
                "asynq:{critical}:processed".to_owned(),
            ],
            vec![
                RedisArg::String("task-id".to_owned()),
                RedisArg::I64(1_707_776_000),
                RedisArg::I64(i64::MAX),
            ],
        );

        let result = executor.eval_script_status(&call).unwrap();

        assert_eq!(result, "OK");
        assert_eq!(
            executor.connection().script_status_calls,
            [(
                RedisScript::Done,
                call.keys().to_vec(),
                call.args().to_vec()
            )]
        );
    }

    #[test]
    fn maps_connection_errors() {
        let mut executor = RedisConnectionExecutor::new(FakeConnection {
            sadd_error: Some("connection closed".to_owned()),
            ..FakeConnection::default()
        });

        let error = executor.sadd("asynq:queues", "critical").unwrap_err();

        assert_eq!(error.message(), "connection closed");
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum ProviderCall {
        GetConnection,
        Sadd {
            key: String,
            member: String,
        },
        ZaddExisting {
            key: String,
            score: i64,
            member: String,
        },
        EvalScriptInt {
            script: RedisScript,
            keys: Vec<String>,
            args: Vec<RedisArg>,
        },
        EvalScriptBytes {
            script: RedisScript,
            keys: Vec<String>,
            args: Vec<RedisArg>,
        },
        EvalScriptStatus {
            script: RedisScript,
            keys: Vec<String>,
            args: Vec<RedisArg>,
        },
    }

    #[derive(Debug, Default)]
    struct FakeProvider {
        calls: Rc<RefCell<Vec<ProviderCall>>>,
        connection_error: Option<String>,
        command_error: Option<String>,
        script_int_results: Rc<RefCell<Vec<i64>>>,
        script_bytes_results: Rc<RefCell<Vec<Option<Vec<u8>>>>>,
        script_status_results: Rc<RefCell<Vec<String>>>,
    }

    #[derive(Debug)]
    struct FakeProviderConnection {
        calls: Rc<RefCell<Vec<ProviderCall>>>,
        command_error: Option<String>,
        script_int_results: Rc<RefCell<Vec<i64>>>,
        script_bytes_results: Rc<RefCell<Vec<Option<Vec<u8>>>>>,
        script_status_results: Rc<RefCell<Vec<String>>>,
    }

    impl RedisConnectionProvider for FakeProvider {
        type Connection = FakeProviderConnection;
        type Error = FakeError;

        fn get_connection(&mut self) -> Result<Self::Connection, Self::Error> {
            self.calls.borrow_mut().push(ProviderCall::GetConnection);
            if let Some(error) = &self.connection_error {
                return Err(FakeError(error.clone()));
            }
            Ok(FakeProviderConnection {
                calls: Rc::clone(&self.calls),
                command_error: self.command_error.clone(),
                script_int_results: Rc::clone(&self.script_int_results),
                script_bytes_results: Rc::clone(&self.script_bytes_results),
                script_status_results: Rc::clone(&self.script_status_results),
            })
        }
    }

    impl RedisCommandExecutor for FakeProviderConnection {
        type Error = FakeError;

        fn sadd(&mut self, key: &str, member: &str) -> Result<(), Self::Error> {
            self.calls.borrow_mut().push(ProviderCall::Sadd {
                key: key.to_owned(),
                member: member.to_owned(),
            });
            if let Some(error) = &self.command_error {
                return Err(FakeError(error.clone()));
            }
            Ok(())
        }

        fn zadd_existing(
            &mut self,
            key: &str,
            score: i64,
            member: &str,
        ) -> Result<usize, Self::Error> {
            self.calls.borrow_mut().push(ProviderCall::ZaddExisting {
                key: key.to_owned(),
                score,
                member: member.to_owned(),
            });
            if let Some(error) = &self.command_error {
                return Err(FakeError(error.clone()));
            }
            Ok(1)
        }

        fn eval_script_int(&mut self, call: &RedisScriptCall) -> Result<i64, Self::Error> {
            self.calls.borrow_mut().push(ProviderCall::EvalScriptInt {
                script: call.script(),
                keys: call.keys().to_vec(),
                args: call.args().to_vec(),
            });
            if let Some(error) = &self.command_error {
                return Err(FakeError(error.clone()));
            }
            Ok(self.script_int_results.borrow_mut().pop().unwrap_or(1))
        }

        fn eval_script_bytes(
            &mut self,
            call: &RedisDequeueCall,
        ) -> Result<Option<Vec<u8>>, Self::Error> {
            self.calls.borrow_mut().push(ProviderCall::EvalScriptBytes {
                script: call.script(),
                keys: call.keys().to_vec(),
                args: call.args().to_vec(),
            });
            if let Some(error) = &self.command_error {
                return Err(FakeError(error.clone()));
            }
            Ok(self.script_bytes_results.borrow_mut().pop().unwrap_or(None))
        }

        fn eval_script_byte_vec(
            &mut self,
            call: &RedisScriptCall,
        ) -> Result<Vec<Vec<u8>>, Self::Error> {
            self.calls.borrow_mut().push(ProviderCall::EvalScriptInt {
                script: call.script(),
                keys: call.keys().to_vec(),
                args: call.args().to_vec(),
            });
            if let Some(error) = &self.command_error {
                return Err(FakeError(error.clone()));
            }
            Ok(self
                .script_bytes_results
                .borrow_mut()
                .pop()
                .flatten()
                .map(|data| vec![data])
                .unwrap_or_default())
        }

        fn eval_script_status(&mut self, call: &RedisScriptCall) -> Result<String, Self::Error> {
            self.calls
                .borrow_mut()
                .push(ProviderCall::EvalScriptStatus {
                    script: call.script(),
                    keys: call.keys().to_vec(),
                    args: call.args().to_vec(),
                });
            if let Some(error) = &self.command_error {
                return Err(FakeError(error.clone()));
            }
            Ok(self
                .script_status_results
                .borrow_mut()
                .pop()
                .unwrap_or_else(|| "OK".to_owned()))
        }
    }

    #[test]
    fn provider_executor_fetches_connection_for_each_operation() {
        let provider = FakeProvider {
            script_int_results: Rc::new(RefCell::new(vec![1])),
            ..FakeProvider::default()
        };
        let calls = Rc::clone(&provider.calls);
        let mut executor = RedisConnectionProviderExecutor::new(provider);
        let keys = vec![
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:pending".to_owned(),
        ];
        let args = vec![
            RedisArg::Bytes(b"message".to_vec()),
            RedisArg::String("task-id".to_owned()),
            RedisArg::I64(1_700_000_000),
        ];
        let call = RedisScriptCall::new(RedisScript::Enqueue, keys, args);

        executor.sadd("asynq:queues", "critical").unwrap();
        let result = executor.eval_script_int(&call).unwrap();

        assert_eq!(result, 1);
        assert_eq!(
            *calls.borrow(),
            [
                ProviderCall::GetConnection,
                ProviderCall::Sadd {
                    key: "asynq:queues".to_owned(),
                    member: "critical".to_owned()
                },
                ProviderCall::GetConnection,
                ProviderCall::EvalScriptInt {
                    script: RedisScript::Enqueue,
                    keys: call.keys().to_vec(),
                    args: call.args().to_vec()
                }
            ]
        );
    }

    #[test]
    fn provider_executor_evals_byte_script() {
        let provider = FakeProvider {
            script_bytes_results: Rc::new(RefCell::new(vec![Some(b"message".to_vec())])),
            ..FakeProvider::default()
        };
        let calls = Rc::clone(&provider.calls);
        let mut executor = RedisConnectionProviderExecutor::new(provider);
        let call = dequeue_call();

        let result = executor.eval_script_bytes(&call).unwrap();

        assert_eq!(result, Some(b"message".to_vec()));
        assert_eq!(
            *calls.borrow(),
            [
                ProviderCall::GetConnection,
                ProviderCall::EvalScriptBytes {
                    script: RedisScript::Dequeue,
                    keys: call.keys().to_vec(),
                    args: call.args().to_vec()
                }
            ]
        );
    }

    #[test]
    fn provider_executor_evals_status_script() {
        let provider = FakeProvider {
            script_status_results: Rc::new(RefCell::new(vec!["OK".to_owned()])),
            ..FakeProvider::default()
        };
        let calls = Rc::clone(&provider.calls);
        let mut executor = RedisConnectionProviderExecutor::new(provider);
        let call = RedisScriptCall::new(
            RedisScript::Done,
            vec![
                "asynq:{critical}:active".to_owned(),
                "asynq:{critical}:lease".to_owned(),
                "asynq:{critical}:t:task-id".to_owned(),
                "asynq:{critical}:processed:2023-11-14".to_owned(),
                "asynq:{critical}:processed".to_owned(),
            ],
            vec![
                RedisArg::String("task-id".to_owned()),
                RedisArg::I64(1_707_776_000),
                RedisArg::I64(i64::MAX),
            ],
        );

        let result = executor.eval_script_status(&call).unwrap();

        assert_eq!(result, "OK");
        assert_eq!(
            *calls.borrow(),
            [
                ProviderCall::GetConnection,
                ProviderCall::EvalScriptStatus {
                    script: RedisScript::Done,
                    keys: call.keys().to_vec(),
                    args: call.args().to_vec()
                }
            ]
        );
    }

    #[test]
    fn provider_executor_maps_connection_errors() {
        let provider = FakeProvider {
            connection_error: Some("cannot connect".to_owned()),
            ..FakeProvider::default()
        };
        let mut executor = RedisConnectionProviderExecutor::new(provider);

        let error = executor.sadd("asynq:queues", "critical").unwrap_err();

        assert_eq!(error.message(), "cannot connect");
    }

    #[test]
    fn provider_executor_maps_command_errors() {
        let provider = FakeProvider {
            command_error: Some("command failed".to_owned()),
            ..FakeProvider::default()
        };
        let mut executor = RedisConnectionProviderExecutor::new(provider);

        let error = executor.sadd("asynq:queues", "critical").unwrap_err();

        assert_eq!(error.message(), "command failed");
    }

    fn dequeue_call() -> RedisDequeueCall {
        let queues = vec!["critical".to_owned()];
        RedisDequeuePlan::from_queues(&queues, UNIX_EPOCH + Duration::from_secs(1_700_000_000))
            .unwrap()
            .queue_calls()[0]
            .clone()
    }
}
