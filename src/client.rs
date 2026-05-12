use std::time::SystemTime;

use crate::{EnqueuePlan, EnqueuePlanError, Task, TaskOption, TaskState};

/// Minimal broker interface for the client enqueue path.
///
/// Reference: Asynq v0.26.0 internal `base.Broker` enqueue-related methods:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L371-L419>.
pub trait Broker {
    fn enqueue(&mut self, plan: &EnqueuePlan) -> Result<(), BrokerError>;
}

/// Generates task IDs for enqueue operations.
///
/// Reference: Asynq v0.26.0 `composeOptions` defaults task IDs with
/// `uuid.NewString`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L217-L264>.
pub trait TaskIdGenerator {
    fn generate_task_id(&mut self) -> String;
}

pub trait Clock {
    fn now(&self) -> SystemTime;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct UuidTaskIdGenerator;

impl TaskIdGenerator for UuidTaskIdGenerator {
    fn generate_task_id(&mut self) -> String {
        uuid::Uuid::new_v4().to_string()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// Client API for enqueueing tasks through a broker.
///
/// Reference: Asynq v0.26.0 `Client.Enqueue` and `Client.EnqueueContext`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L266-L331>.
#[derive(Debug, Clone)]
pub struct Client<B, G = UuidTaskIdGenerator, C = SystemClock> {
    broker: B,
    task_id_generator: G,
    clock: C,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnqueueResult {
    id: String,
    queue: String,
    state: TaskState,
    next_process_at: Option<SystemTime>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientError {
    Plan(EnqueuePlanError),
    Broker(BrokerError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrokerError {
    DuplicateTask,
    TaskIdConflict,
    Other(String),
}

impl<B> Client<B, UuidTaskIdGenerator, SystemClock> {
    pub fn new(broker: B) -> Self {
        Self::with_parts(broker, UuidTaskIdGenerator, SystemClock)
    }
}

impl<B, G> Client<B, G, SystemClock> {
    pub fn with_task_id_generator(broker: B, task_id_generator: G) -> Self {
        Self::with_parts(broker, task_id_generator, SystemClock)
    }
}

impl<B, G, C> Client<B, G, C> {
    pub fn with_parts(broker: B, task_id_generator: G, clock: C) -> Self {
        Self {
            broker,
            task_id_generator,
            clock,
        }
    }

    pub fn broker(&self) -> &B {
        &self.broker
    }

    pub fn broker_mut(&mut self) -> &mut B {
        &mut self.broker
    }

    pub fn into_broker(self) -> B {
        self.broker
    }
}

impl<B, G, C> Client<B, G, C>
where
    B: Broker,
    G: TaskIdGenerator,
    C: Clock,
{
    pub fn enqueue(&mut self, task: &Task) -> Result<EnqueueResult, ClientError> {
        self.enqueue_with_options(task, std::iter::empty())
    }

    pub fn enqueue_with_options<I>(
        &mut self,
        task: &Task,
        options: I,
    ) -> Result<EnqueueResult, ClientError>
    where
        I: IntoIterator<Item = TaskOption>,
    {
        let now = self.clock.now();
        let generated_task_id = self.task_id_generator.generate_task_id();
        let plan = EnqueuePlan::from_task_with_options(task, options, now, generated_task_id)?;

        self.broker.enqueue(&plan)?;

        Ok(EnqueueResult::from_plan(&plan))
    }
}

impl EnqueueResult {
    pub fn from_plan(plan: &EnqueuePlan) -> Self {
        let msg = plan.message();
        Self {
            id: msg.id.clone(),
            queue: msg.queue.clone(),
            state: plan.state(),
            next_process_at: plan.next_process_at(),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn queue(&self) -> &str {
        &self.queue
    }

    pub fn state(&self) -> TaskState {
        self.state
    }

    pub fn next_process_at(&self) -> Option<SystemTime> {
        self.next_process_at
    }
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plan(error) => write!(f, "failed to build enqueue plan: {error}"),
            Self::Broker(error) => write!(f, "broker failed to enqueue task: {error}"),
        }
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Plan(error) => Some(error),
            Self::Broker(error) => Some(error),
        }
    }
}

impl From<EnqueuePlanError> for ClientError {
    fn from(error: EnqueuePlanError) -> Self {
        Self::Plan(error)
    }
}

impl From<BrokerError> for ClientError {
    fn from(error: BrokerError) -> Self {
        Self::Broker(error)
    }
}

impl std::fmt::Display for BrokerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateTask => f.write_str("task already exists"),
            Self::TaskIdConflict => f.write_str("task ID conflicts with another task"),
            Self::Other(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for BrokerError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    #[derive(Debug, Default)]
    struct RecordingBroker {
        plans: Vec<EnqueuePlan>,
        error: Option<BrokerError>,
    }

    impl Broker for RecordingBroker {
        fn enqueue(&mut self, plan: &EnqueuePlan) -> Result<(), BrokerError> {
            if let Some(error) = self.error.clone() {
                return Err(error);
            }
            self.plans.push(plan.clone());
            Ok(())
        }
    }

    #[derive(Debug)]
    struct FixedTaskIdGenerator(&'static str);

    impl TaskIdGenerator for FixedTaskIdGenerator {
        fn generate_task_id(&mut self) -> String {
            self.0.to_owned()
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct FixedClock(SystemTime);

    impl Clock for FixedClock {
        fn now(&self) -> SystemTime {
            self.0
        }
    }

    #[test]
    fn enqueue_records_plan_and_returns_result() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let task = Task::new_with_options(
            "email:welcome",
            b"payload".to_vec(),
            [TaskOption::queue("critical")],
        );
        let mut client = Client::with_parts(
            RecordingBroker::default(),
            FixedTaskIdGenerator("task-id"),
            FixedClock(now),
        );

        let result = client.enqueue(&task).unwrap();

        assert_eq!(result.id(), "task-id");
        assert_eq!(result.queue(), "critical");
        assert_eq!(result.state(), TaskState::Pending);
        assert_eq!(result.next_process_at(), Some(now));

        let plans = &client.broker().plans;
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].message().id, "task-id");
        assert_eq!(plans[0].message().queue, "critical");
        assert_eq!(plans[0].state(), TaskState::Pending);
    }

    #[test]
    fn enqueue_options_override_task_options() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let task = Task::new_with_options(
            "email:welcome",
            Vec::new(),
            [
                TaskOption::queue("low"),
                TaskOption::task_id("task-option-id"),
            ],
        );
        let mut client = Client::with_parts(
            RecordingBroker::default(),
            FixedTaskIdGenerator("generated-id"),
            FixedClock(now),
        );

        let result = client
            .enqueue_with_options(
                &task,
                [
                    TaskOption::queue("critical"),
                    TaskOption::task_id("enqueue-option-id"),
                ],
            )
            .unwrap();

        assert_eq!(result.id(), "enqueue-option-id");
        assert_eq!(result.queue(), "critical");
        assert_eq!(client.broker().plans[0].message().id, "enqueue-option-id");
    }

    #[test]
    fn process_in_uses_client_clock() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let task = Task::new("email:welcome", Vec::new());
        let mut client = Client::with_parts(
            RecordingBroker::default(),
            FixedTaskIdGenerator("task-id"),
            FixedClock(now),
        );

        let result = client
            .enqueue_with_options(&task, [TaskOption::process_in(Duration::from_secs(60))])
            .unwrap();

        assert_eq!(result.state(), TaskState::Scheduled);
        assert_eq!(
            result.next_process_at(),
            Some(now + Duration::from_secs(60))
        );
        assert_eq!(client.broker().plans[0].state(), TaskState::Scheduled);
    }

    #[test]
    fn plan_errors_happen_before_broker_enqueue() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let task = Task::new(" ", Vec::new());
        let mut client = Client::with_parts(
            RecordingBroker::default(),
            FixedTaskIdGenerator("task-id"),
            FixedClock(now),
        );

        let error = client.enqueue(&task).unwrap_err();

        assert_eq!(error, ClientError::Plan(EnqueuePlanError::EmptyTaskType));
        assert!(client.broker().plans.is_empty());
    }

    #[test]
    fn broker_errors_are_propagated() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let task = Task::new("email:welcome", Vec::new());
        let broker = RecordingBroker {
            plans: Vec::new(),
            error: Some(BrokerError::TaskIdConflict),
        };
        let mut client =
            Client::with_parts(broker, FixedTaskIdGenerator("task-id"), FixedClock(now));

        let error = client.enqueue(&task).unwrap_err();

        assert_eq!(error, ClientError::Broker(BrokerError::TaskIdConflict));
        assert!(client.broker().plans.is_empty());
    }
}
