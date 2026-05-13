use crate::{RedisArg, RedisEnqueueScript, RedisExecutor, RedisExecutorError};

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
}
