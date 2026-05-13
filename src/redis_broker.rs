use std::time::SystemTime;

use crate::{
    Broker, BrokerError, Clock, EnqueuePlan, RedisArg, RedisEnqueueOperation, RedisEnqueuePlan,
    RedisEnqueuePlanError, RedisEnqueueScript, RedisScriptCall, RedisScriptCallError,
    RedisScriptResult, SystemClock,
};

/// Minimal executor surface needed by `RedisBroker`.
///
/// Reference: Asynq v0.26.0 RDB enqueue methods execute a queue publication
/// plus an enqueue Lua script:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L145>.
pub trait RedisExecutor {
    fn sadd(&mut self, key: &str, member: &str) -> Result<(), RedisExecutorError>;

    fn run_enqueue_script(
        &mut self,
        script: RedisEnqueueScript,
        keys: &[String],
        args: &[RedisArg],
    ) -> Result<i64, RedisExecutorError>;
}

#[derive(Debug, Clone)]
pub struct RedisBroker<E, C = SystemClock> {
    executor: E,
    clock: C,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisExecutorError {
    message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedisBrokerError {
    Plan(RedisEnqueuePlanError),
    ScriptCall(RedisScriptCallError),
    Executor(RedisExecutorError),
    UnexpectedScriptResult {
        script: RedisEnqueueScript,
        result: i64,
    },
}

impl<E> RedisBroker<E, SystemClock> {
    pub fn new(executor: E) -> Self {
        Self::with_clock(executor, SystemClock)
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

impl<E, C> Broker for RedisBroker<E, C>
where
    E: RedisExecutor,
    C: Clock,
{
    fn enqueue(&mut self, plan: &EnqueuePlan) -> Result<(), BrokerError> {
        self.enqueue_with_now(plan, self.clock.now())
    }
}

impl<E, C> RedisBroker<E, C>
where
    E: RedisExecutor,
{
    pub fn enqueue_with_now(
        &mut self,
        plan: &EnqueuePlan,
        now: SystemTime,
    ) -> Result<(), BrokerError> {
        let redis_plan = RedisEnqueuePlan::from_enqueue_plan(plan, now)
            .map_err(RedisBrokerError::Plan)
            .map_err(BrokerError::from)?;

        for operation in redis_plan.operations() {
            self.execute(operation)?;
        }
        Ok(())
    }

    fn execute(&mut self, operation: &RedisEnqueueOperation) -> Result<(), BrokerError> {
        match operation {
            RedisEnqueueOperation::PublishQueue { key, queue } => {
                self.executor
                    .sadd(key, queue)
                    .map_err(RedisBrokerError::Executor)
                    .map_err(BrokerError::from)?;
                Ok(())
            }
            RedisEnqueueOperation::RunScript(call) => {
                call.validate()
                    .map_err(RedisBrokerError::ScriptCall)
                    .map_err(BrokerError::from)?;
                let result = self
                    .executor
                    .run_enqueue_script(call.script(), call.keys(), call.args())
                    .map_err(RedisBrokerError::Executor)
                    .map_err(BrokerError::from)?;
                map_script_result(call, result)
            }
        }
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

impl RedisExecutorError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for RedisExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for RedisExecutorError {}

impl std::fmt::Display for RedisBrokerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plan(error) => write!(f, "failed to build Redis enqueue plan: {error}"),
            Self::ScriptCall(error) => write!(f, "invalid Redis script call: {error}"),
            Self::Executor(error) => write!(f, "Redis executor failed: {error}"),
            Self::UnexpectedScriptResult { script, result } => {
                write!(f, "unexpected {script:?} script result: {result}")
            }
        }
    }
}

impl std::error::Error for RedisBrokerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Plan(error) => Some(error),
            Self::ScriptCall(error) => Some(error),
            Self::Executor(error) => Some(error),
            Self::UnexpectedScriptResult { .. } => None,
        }
    }
}

impl From<RedisEnqueuePlanError> for RedisBrokerError {
    fn from(error: RedisEnqueuePlanError) -> Self {
        Self::Plan(error)
    }
}

impl From<RedisExecutorError> for RedisBrokerError {
    fn from(error: RedisExecutorError) -> Self {
        Self::Executor(error)
    }
}

impl From<RedisScriptCallError> for RedisBrokerError {
    fn from(error: RedisScriptCallError) -> Self {
        Self::ScriptCall(error)
    }
}

impl From<RedisBrokerError> for BrokerError {
    fn from(error: RedisBrokerError) -> Self {
        match error {
            RedisBrokerError::Plan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ScriptCall(error) => Self::Other(error.to_string()),
            RedisBrokerError::Executor(error) => Self::Other(error.to_string()),
            RedisBrokerError::UnexpectedScriptResult { script, result } => {
                Self::Other(format!("unexpected {script:?} script result: {result}"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    use crate::{Task, TaskOption, TaskState};

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum ExecutorCall {
        Sadd {
            key: String,
            member: String,
        },
        RunScript {
            script: RedisEnqueueScript,
            keys: Vec<String>,
            args: Vec<RedisArg>,
        },
    }

    #[derive(Debug)]
    struct FakeExecutor {
        calls: Vec<ExecutorCall>,
        script_results: Vec<i64>,
        sadd_error: Option<RedisExecutorError>,
        script_error: Option<RedisExecutorError>,
    }

    impl Default for FakeExecutor {
        fn default() -> Self {
            Self {
                calls: Vec::new(),
                script_results: vec![1],
                sadd_error: None,
                script_error: None,
            }
        }
    }

    impl RedisExecutor for FakeExecutor {
        fn sadd(&mut self, key: &str, member: &str) -> Result<(), RedisExecutorError> {
            self.calls.push(ExecutorCall::Sadd {
                key: key.to_owned(),
                member: member.to_owned(),
            });
            if let Some(error) = self.sadd_error.clone() {
                return Err(error);
            }
            Ok(())
        }

        fn run_enqueue_script(
            &mut self,
            script: RedisEnqueueScript,
            keys: &[String],
            args: &[RedisArg],
        ) -> Result<i64, RedisExecutorError> {
            self.calls.push(ExecutorCall::RunScript {
                script,
                keys: keys.to_vec(),
                args: args.to_vec(),
            });
            if let Some(error) = self.script_error.clone() {
                return Err(error);
            }
            Ok(self.script_results.remove(0))
        }
    }

    #[test]
    fn executes_publish_then_enqueue_script() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let task = Task::new_with_options(
            "email:welcome",
            b"payload".to_vec(),
            [TaskOption::queue("critical")],
        );
        let plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();
        let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

        broker.enqueue(&plan).unwrap();

        let calls = &broker.executor().calls;
        assert_eq!(calls.len(), 2);
        assert_eq!(
            calls[0],
            ExecutorCall::Sadd {
                key: "asynq:queues".to_owned(),
                member: "critical".to_owned()
            }
        );
        let ExecutorCall::RunScript { script, keys, args } = &calls[1] else {
            panic!("expected script call");
        };
        assert_eq!(*script, RedisEnqueueScript::Enqueue);
        assert_eq!(
            keys,
            &[
                "asynq:{critical}:t:task-id".to_owned(),
                "asynq:{critical}:pending".to_owned(),
            ]
        );
        assert!(matches!(args[0], RedisArg::Bytes(_)));
        assert_eq!(args[1], RedisArg::String("task-id".to_owned()));
        assert_eq!(args[2], RedisArg::I64(1_700_000_000_000_000_000));
    }

    #[test]
    fn executes_unique_scheduled_script() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let task = Task::new_with_options(
            "email:welcome",
            b"payload".to_vec(),
            [
                TaskOption::queue("critical"),
                TaskOption::process_in(Duration::from_secs(60)),
                TaskOption::unique(Duration::from_secs(300)),
            ],
        );
        let plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();
        assert_eq!(plan.state(), TaskState::Scheduled);
        let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

        broker.enqueue(&plan).unwrap();

        let ExecutorCall::RunScript { script, keys, args } = &broker.executor().calls[1] else {
            panic!("expected script call");
        };
        assert_eq!(*script, RedisEnqueueScript::ScheduleUnique);
        assert_eq!(
            keys,
            &[
                "asynq:{critical}:unique:email:welcome:321c3cf486ed509164edec1e1981fec8".to_owned(),
                "asynq:{critical}:t:task-id".to_owned(),
                "asynq:{critical}:scheduled".to_owned(),
            ]
        );
        assert_eq!(args[0], RedisArg::String("task-id".to_owned()));
        assert_eq!(args[1], RedisArg::I64(360));
        assert_eq!(args[2], RedisArg::I64(1_700_000_060));
        assert!(matches!(args[3], RedisArg::Bytes(_)));
    }

    #[test]
    fn maps_unique_duplicate_result() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let task = Task::new_with_options(
            "email:welcome",
            b"payload".to_vec(),
            [TaskOption::unique(Duration::from_secs(300))],
        );
        let plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();
        let executor = FakeExecutor {
            script_results: vec![-1],
            ..FakeExecutor::default()
        };
        let mut broker = RedisBroker::with_clock(executor, TestClock(now));

        let error = broker.enqueue(&plan).unwrap_err();

        assert_eq!(error, BrokerError::DuplicateTask);
    }

    #[test]
    fn maps_task_id_conflict_result() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let task = Task::new("email:welcome", Vec::new());
        let plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();
        let executor = FakeExecutor {
            script_results: vec![0],
            ..FakeExecutor::default()
        };
        let mut broker = RedisBroker::with_clock(executor, TestClock(now));

        let error = broker.enqueue(&plan).unwrap_err();

        assert_eq!(error, BrokerError::TaskIdConflict);
    }

    #[test]
    fn maps_executor_errors() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let task = Task::new("email:welcome", Vec::new());
        let plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();
        let executor = FakeExecutor {
            sadd_error: Some(RedisExecutorError::new("connection closed")),
            ..FakeExecutor::default()
        };
        let mut broker = RedisBroker::with_clock(executor, TestClock(now));

        let error = broker.enqueue(&plan).unwrap_err();

        assert_eq!(error, BrokerError::Other("connection closed".to_owned()));
    }

    #[derive(Debug, Clone, Copy)]
    struct TestClock(SystemTime);

    impl Clock for TestClock {
        fn now(&self) -> SystemTime {
            self.0
        }
    }
}
