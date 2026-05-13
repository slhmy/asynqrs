use crate::{RedisArg, RedisEnqueueScript, RedisExecutor, RedisExecutorError};

/// Provides a Redis connection for command execution.
///
/// This is a Rust-specific adapter boundary for client or pool types. The
/// enqueue commands it feeds are still the upstream Asynq Redis operations.
///
/// Reference: Asynq v0.26.0 RDB enqueue methods use `SAdd` and redis Lua
/// scripts to persist enqueue state:
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
/// Reference: Asynq v0.26.0 RDB enqueue methods use `SAdd` and redis Lua
/// scripts to persist enqueue state:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L735>.
#[derive(Debug, Clone)]
pub struct RedisConnectionExecutor<C> {
    connection: C,
}

pub trait RedisCommandExecutor {
    type Error: std::fmt::Display;

    fn sadd(&mut self, key: &str, member: &str) -> Result<(), Self::Error>;

    fn invoke_script(
        &mut self,
        script: RedisEnqueueScript,
        keys: &[String],
        args: &[RedisArg],
    ) -> Result<i64, Self::Error>;
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

impl<P> RedisExecutor for RedisConnectionProviderExecutor<P>
where
    P: RedisConnectionProvider,
{
    fn sadd(&mut self, key: &str, member: &str) -> Result<(), RedisExecutorError> {
        self.with_connection(|connection| connection.sadd(key, member))
    }

    fn run_enqueue_script(
        &mut self,
        script: RedisEnqueueScript,
        keys: &[String],
        args: &[RedisArg],
    ) -> Result<i64, RedisExecutorError> {
        self.with_connection(|connection| connection.invoke_script(script, keys, args))
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

    fn run_enqueue_script(
        &mut self,
        script: RedisEnqueueScript,
        keys: &[String],
        args: &[RedisArg],
    ) -> Result<i64, RedisExecutorError> {
        self.connection
            .invoke_script(script, keys, args)
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

    fn invoke_script(
        &mut self,
        script: RedisEnqueueScript,
        keys: &[String],
        args: &[RedisArg],
    ) -> Result<i64, Self::Error> {
        let redis_script = redis::Script::new(script.source());
        let mut invocation = redis_script.prepare_invoke();
        for key in keys {
            invocation.key(key);
        }
        for arg in args {
            push_arg(&mut invocation, arg);
        }
        invocation.invoke(self)
    }
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
    use std::cell::RefCell;
    use std::rc::Rc;

    #[derive(Debug, Default)]
    struct FakeConnection {
        sadd_calls: Vec<(String, String)>,
        script_calls: Vec<(RedisEnqueueScript, Vec<String>, Vec<RedisArg>)>,
        script_results: Vec<i64>,
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

        fn invoke_script(
            &mut self,
            script: RedisEnqueueScript,
            keys: &[String],
            args: &[RedisArg],
        ) -> Result<i64, Self::Error> {
            self.script_calls
                .push((script, keys.to_vec(), args.to_vec()));
            if let Some(error) = &self.script_error {
                return Err(FakeError(error.clone()));
            }
            Ok(self.script_results.pop().unwrap_or(1))
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
    fn forwards_script_invocation_to_connection() {
        let mut executor = RedisConnectionExecutor::new(FakeConnection {
            script_results: vec![1],
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

        let result = executor
            .run_enqueue_script(RedisEnqueueScript::Enqueue, &keys, &args)
            .unwrap();

        assert_eq!(result, 1);
        assert_eq!(
            executor.connection().script_calls,
            [(RedisEnqueueScript::Enqueue, keys, args)]
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
        InvokeScript {
            script: RedisEnqueueScript,
            keys: Vec<String>,
            args: Vec<RedisArg>,
        },
    }

    #[derive(Debug, Default)]
    struct FakeProvider {
        calls: Rc<RefCell<Vec<ProviderCall>>>,
        connection_error: Option<String>,
        command_error: Option<String>,
        script_results: Rc<RefCell<Vec<i64>>>,
    }

    #[derive(Debug)]
    struct FakeProviderConnection {
        calls: Rc<RefCell<Vec<ProviderCall>>>,
        command_error: Option<String>,
        script_results: Rc<RefCell<Vec<i64>>>,
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
                script_results: Rc::clone(&self.script_results),
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

        fn invoke_script(
            &mut self,
            script: RedisEnqueueScript,
            keys: &[String],
            args: &[RedisArg],
        ) -> Result<i64, Self::Error> {
            self.calls.borrow_mut().push(ProviderCall::InvokeScript {
                script,
                keys: keys.to_vec(),
                args: args.to_vec(),
            });
            if let Some(error) = &self.command_error {
                return Err(FakeError(error.clone()));
            }
            Ok(self.script_results.borrow_mut().pop().unwrap_or(1))
        }
    }

    #[test]
    fn provider_executor_fetches_connection_for_each_operation() {
        let provider = FakeProvider {
            script_results: Rc::new(RefCell::new(vec![1])),
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

        executor.sadd("asynq:queues", "critical").unwrap();
        let result = executor
            .run_enqueue_script(RedisEnqueueScript::Enqueue, &keys, &args)
            .unwrap();

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
                ProviderCall::InvokeScript {
                    script: RedisEnqueueScript::Enqueue,
                    keys,
                    args
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
}
