use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::{Duration, SystemTime};

use crate::{
    ArchiveBroker, ArchiveError, Clock, CompleteBroker, CompleteError,
    DEFAULT_SERVER_RECOVER_RETRY_DELAY, DequeueBroker, DequeueError, ForwardBroker, ForwardError,
    RecoverBroker, RecoverError, RetryBroker, RetryError, ServerMaintenanceRun, SystemClock, Task,
    TaskMessage,
};

/// Processes a single task.
///
/// Reference: Asynq v0.26.0 public `Handler` and `HandlerFunc` APIs:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L622-L650>.
pub trait Handler {
    fn process_task(&mut self, task: &Task) -> Result<(), HandlerError>;
}

impl<F> Handler for F
where
    F: FnMut(&Task) -> Result<(), HandlerError>,
{
    fn process_task(&mut self, task: &Task) -> Result<(), HandlerError> {
        self(task)
    }
}

/// Error returned by a task handler.
///
/// Reference: Asynq v0.26.0 `SkipRetry` and `RevokeTask` handler sentinel
/// errors:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L327-L348>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandlerError {
    Failed(String),
    SkipRetry(String),
    RevokeTask(String),
    Panic(String),
}

/// Calculates the delay before retrying a failed task.
///
/// Reference: Asynq v0.26.0 `RetryDelayFunc`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L291-L297>.
pub trait RetryDelay {
    fn retry_delay(&mut self, retried: i32, error: &HandlerError, task: &Task) -> Duration;
}

impl<F> RetryDelay for F
where
    F: FnMut(i32, &HandlerError, &Task) -> Duration,
{
    fn retry_delay(&mut self, retried: i32, error: &HandlerError, task: &Task) -> Duration {
        self(retried, error, task)
    }
}

/// Determines whether a handler error counts as a failure in task statistics.
///
/// Reference: Asynq v0.26.0 `Config.IsFailure`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L124-L130>.
pub trait IsFailure {
    fn is_failure(&mut self, error: &HandlerError) -> bool;
}

impl<F> IsFailure for F
where
    F: FnMut(&HandlerError) -> bool,
{
    fn is_failure(&mut self, error: &HandlerError) -> bool {
        self(error)
    }
}

/// Handles errors returned by task handlers.
///
/// Reference: Asynq v0.26.0 `ErrorHandler` and processor error hook:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L277-L287>,
/// <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L335-L338>.
pub trait ErrorHandler {
    fn handle_error(&mut self, task: &Task, error: &HandlerError);
}

impl<F> ErrorHandler for F
where
    F: FnMut(&Task, &HandlerError),
{
    fn handle_error(&mut self, task: &Task, error: &HandlerError) {
        self(task, error);
    }
}

/// Default exponential retry delay.
///
/// Reference: Asynq v0.26.0 `DefaultRetryDelayFunc` uses the Sidekiq-inspired
/// formula `n^4 + 15 + rand(0..30) * (n + 1)` seconds:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L399-L405>.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultRetryDelay;

/// Default failure predicate: every handler error counts as a failure.
///
/// Reference: Asynq v0.26.0 `defaultIsFailureFunc`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L407>.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultIsFailure;

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopErrorHandler;

/// Minimal worker-side processor that runs one dequeued task through a handler
/// and then marks it complete, retry, archive, or done.
///
/// Reference: Asynq v0.26.0 processor success/failure routing:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L221-L381>.
///
/// TODO: Add worker concurrency, task context timeout/deadline handling, lease
/// extension, requeue-on-shutdown, sync retry, and upstream maintenance
/// intervals once the full `Server` / `Processor` runtime is modeled.
#[derive(Debug, Clone)]
pub struct Processor<
    B,
    H,
    R = DefaultRetryDelay,
    C = SystemClock,
    I = DefaultIsFailure,
    E = NoopErrorHandler,
> {
    broker: B,
    handler: H,
    retry_delay: R,
    clock: C,
    is_failure: I,
    error_handler: E,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessorRun {
    Completed {
        task_id: String,
    },
    Retried {
        task_id: String,
        retry_at: SystemTime,
    },
    Archived {
        task_id: String,
    },
    Revoked {
        task_id: String,
    },
    NoProcessableTask,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessorError {
    Dequeue(DequeueError),
    Complete(CompleteError),
    Retry(RetryError),
    Archive(ArchiveError),
    Forward(ForwardError),
    Recover(RecoverError),
    TimeOverflow(&'static str),
}

impl<B, H> Processor<B, H, DefaultRetryDelay, SystemClock> {
    pub fn new(broker: B, handler: H) -> Self {
        Self::with_parts(broker, handler, DefaultRetryDelay, SystemClock)
    }
}

impl<B, H, R> Processor<B, H, R, SystemClock> {
    pub fn with_retry_delay(broker: B, handler: H, retry_delay: R) -> Self {
        Self::with_parts(broker, handler, retry_delay, SystemClock)
    }
}

impl<B, H, R, C> Processor<B, H, R, C> {
    pub fn with_parts(broker: B, handler: H, retry_delay: R, clock: C) -> Self {
        Self::with_parts_and_hooks(
            broker,
            handler,
            retry_delay,
            clock,
            DefaultIsFailure,
            NoopErrorHandler,
        )
    }
}

impl<B, H, R, C, I, E> Processor<B, H, R, C, I, E> {
    pub fn with_parts_and_hooks(
        broker: B,
        handler: H,
        retry_delay: R,
        clock: C,
        is_failure: I,
        error_handler: E,
    ) -> Self {
        Self {
            broker,
            handler,
            retry_delay,
            clock,
            is_failure,
            error_handler,
        }
    }

    pub fn with_is_failure<I2>(self, is_failure: I2) -> Processor<B, H, R, C, I2, E> {
        Processor {
            broker: self.broker,
            handler: self.handler,
            retry_delay: self.retry_delay,
            clock: self.clock,
            is_failure,
            error_handler: self.error_handler,
        }
    }

    pub fn with_error_handler<E2>(self, error_handler: E2) -> Processor<B, H, R, C, I, E2> {
        Processor {
            broker: self.broker,
            handler: self.handler,
            retry_delay: self.retry_delay,
            clock: self.clock,
            is_failure: self.is_failure,
            error_handler,
        }
    }

    pub fn broker(&self) -> &B {
        &self.broker
    }

    pub fn broker_mut(&mut self) -> &mut B {
        &mut self.broker
    }

    pub fn handler(&self) -> &H {
        &self.handler
    }

    pub fn handler_mut(&mut self) -> &mut H {
        &mut self.handler
    }

    pub fn is_failure(&self) -> &I {
        &self.is_failure
    }

    pub fn is_failure_mut(&mut self) -> &mut I {
        &mut self.is_failure
    }

    pub fn error_handler(&self) -> &E {
        &self.error_handler
    }

    pub fn error_handler_mut(&mut self) -> &mut E {
        &mut self.error_handler
    }

    pub fn into_broker(self) -> B {
        self.broker
    }
}

impl<B, H, R, C, I, E> Processor<B, H, R, C, I, E>
where
    B: DequeueBroker + CompleteBroker + RetryBroker + ArchiveBroker,
    H: Handler,
    R: RetryDelay,
    C: Clock,
    I: IsFailure,
    E: ErrorHandler,
{
    pub fn run_once(&mut self, queues: &[String]) -> Result<ProcessorRun, ProcessorError> {
        let dequeued = match self.broker.dequeue(queues) {
            Ok(task) => task,
            Err(DequeueError::NoProcessableTask) => return Ok(ProcessorRun::NoProcessableTask),
            Err(error) => return Err(ProcessorError::Dequeue(error)),
        };

        let message = dequeued.message().clone();
        let task = task_from_message(&message);
        match perform(&mut self.handler, &task) {
            Ok(()) => {
                self.broker.complete(&message)?;
                Ok(ProcessorRun::Completed {
                    task_id: message.id,
                })
            }
            Err(error) => {
                self.error_handler.handle_error(&task, &error);
                match error {
                    HandlerError::RevokeTask(_) => {
                        let mut revoked = message.clone();
                        revoked.retention = 0;
                        self.broker.complete(&revoked)?;
                        Ok(ProcessorRun::Revoked {
                            task_id: message.id,
                        })
                    }
                    error @ HandlerError::SkipRetry(_) => {
                        let error_message = error.to_string();
                        self.broker
                            .archive(&message, self.clock.now(), &error_message, true)?;
                        Ok(ProcessorRun::Archived {
                            task_id: message.id,
                        })
                    }
                    error if message.retried >= message.retry => {
                        let error_message = error.to_string();
                        self.broker
                            .archive(&message, self.clock.now(), &error_message, true)?;
                        Ok(ProcessorRun::Archived {
                            task_id: message.id,
                        })
                    }
                    error => {
                        let delay = self.retry_delay.retry_delay(message.retried, &error, &task);
                        let retry_at = self
                            .clock
                            .now()
                            .checked_add(delay)
                            .ok_or(ProcessorError::TimeOverflow("retry time"))?;
                        let error_message = error.to_string();
                        let is_failure = self.is_failure.is_failure(&error);
                        self.broker
                            .retry(&message, retry_at, &error_message, is_failure)?;
                        Ok(ProcessorRun::Retried {
                            task_id: message.id,
                            retry_at,
                        })
                    }
                }
            }
        }
    }

    pub fn run_until_idle(&mut self, queues: &[String]) -> Result<usize, ProcessorError> {
        let mut processed = 0;
        loop {
            match self.run_once(queues)? {
                ProcessorRun::NoProcessableTask => return Ok(processed),
                _ => processed += 1,
            }
        }
    }
}

impl<B, H, R, C, I, E> Processor<B, H, R, C, I, E>
where
    B: ForwardBroker + RecoverBroker,
    C: Clock,
{
    /// Runs one synchronous server-maintenance pass.
    ///
    /// Reference: Asynq v0.26.0 starts background forwarder and recoverer
    /// components from `Server.Start`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L687-L695>.
    ///
    /// TODO: Replace the fixed retry delay with upstream retry-delay
    /// calculation and separate polling intervals when async server runtime is
    /// modeled.
    pub fn run_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessorError> {
        let mut forwarded_scheduled = 0;
        let mut forwarded_retry = 0;
        let mut recovered_retried = 0;
        let mut recovered_archived = 0;
        let retry_at = self
            .clock
            .now()
            .checked_add(DEFAULT_SERVER_RECOVER_RETRY_DELAY)
            .ok_or(ProcessorError::TimeOverflow("recovery retry time"))?;

        for queue in queues {
            forwarded_scheduled += self.broker.forward_scheduled(queue)?;
            forwarded_retry += self.broker.forward_retry(queue)?;
            let recovered =
                self.broker
                    .recover_expired_leases(queue, retry_at, "task lease expired")?;
            recovered_retried += recovered.retried();
            recovered_archived += recovered.archived();
        }

        Ok(ServerMaintenanceRun::new(
            forwarded_scheduled,
            forwarded_retry,
            recovered_retried,
            recovered_archived,
        ))
    }
}

impl DefaultRetryDelay {
    pub fn delay_for_retried_count(retried: i32) -> Duration {
        let n = retried.max(0) as u64;
        let jitter = rand::random_range(0..30_u64);
        Duration::from_secs(
            n.saturating_pow(4)
                .saturating_add(15)
                .saturating_add(jitter.saturating_mul(n.saturating_add(1))),
        )
    }
}

impl RetryDelay for DefaultRetryDelay {
    fn retry_delay(&mut self, retried: i32, _error: &HandlerError, _task: &Task) -> Duration {
        Self::delay_for_retried_count(retried)
    }
}

impl IsFailure for DefaultIsFailure {
    fn is_failure(&mut self, _error: &HandlerError) -> bool {
        true
    }
}

impl ErrorHandler for NoopErrorHandler {
    fn handle_error(&mut self, _task: &Task, _error: &HandlerError) {}
}

fn perform<H>(handler: &mut H, task: &Task) -> Result<(), HandlerError>
where
    H: Handler,
{
    catch_unwind(AssertUnwindSafe(|| handler.process_task(task))).unwrap_or_else(|panic| {
        let message = if let Some(message) = panic.downcast_ref::<&str>() {
            (*message).to_owned()
        } else if let Some(message) = panic.downcast_ref::<String>() {
            message.clone()
        } else {
            "task handler panicked".to_owned()
        };
        Err(HandlerError::Panic(message))
    })
}

fn task_from_message(message: &TaskMessage) -> Task {
    Task::with_headers(
        message.r#type.clone(),
        message.payload.clone(),
        message.headers.clone(),
    )
}

impl HandlerError {
    pub fn failed(message: impl Into<String>) -> Self {
        Self::Failed(message.into())
    }

    pub fn skip_retry(message: impl Into<String>) -> Self {
        Self::SkipRetry(message.into())
    }

    pub fn revoke_task(message: impl Into<String>) -> Self {
        Self::RevokeTask(message.into())
    }
}

impl std::fmt::Display for HandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failed(message)
            | Self::SkipRetry(message)
            | Self::RevokeTask(message)
            | Self::Panic(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for HandlerError {}

impl std::fmt::Display for ProcessorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dequeue(error) => write!(f, "failed to dequeue task: {error}"),
            Self::Complete(error) => write!(f, "failed to complete task: {error}"),
            Self::Retry(error) => write!(f, "failed to retry task: {error}"),
            Self::Archive(error) => write!(f, "failed to archive task: {error}"),
            Self::Forward(error) => write!(f, "failed to forward ready tasks: {error}"),
            Self::Recover(error) => write!(f, "failed to recover expired leases: {error}"),
            Self::TimeOverflow(context) => write!(f, "{context} overflowed"),
        }
    }
}

impl std::error::Error for ProcessorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Dequeue(error) => Some(error),
            Self::Complete(error) => Some(error),
            Self::Retry(error) => Some(error),
            Self::Archive(error) => Some(error),
            Self::Forward(error) => Some(error),
            Self::Recover(error) => Some(error),
            Self::TimeOverflow(_) => None,
        }
    }
}

impl From<DequeueError> for ProcessorError {
    fn from(error: DequeueError) -> Self {
        Self::Dequeue(error)
    }
}

impl From<CompleteError> for ProcessorError {
    fn from(error: CompleteError) -> Self {
        Self::Complete(error)
    }
}

impl From<RetryError> for ProcessorError {
    fn from(error: RetryError) -> Self {
        Self::Retry(error)
    }
}

impl From<ArchiveError> for ProcessorError {
    fn from(error: ArchiveError) -> Self {
        Self::Archive(error)
    }
}

impl From<ForwardError> for ProcessorError {
    fn from(error: ForwardError) -> Self {
        Self::Forward(error)
    }
}

impl From<RecoverError> for ProcessorError {
    fn from(error: RecoverError) -> Self {
        Self::Recover(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DequeuedTask;

    #[derive(Debug, Default)]
    struct RecordingBroker {
        dequeued: Vec<Result<DequeuedTask, DequeueError>>,
        completed: Vec<String>,
        retried: Vec<(String, SystemTime, String, bool)>,
        archived: Vec<(String, SystemTime, String, bool)>,
    }

    impl DequeueBroker for RecordingBroker {
        fn dequeue(&mut self, _queues: &[String]) -> Result<DequeuedTask, DequeueError> {
            self.dequeued.remove(0)
        }
    }

    impl CompleteBroker for RecordingBroker {
        fn complete(&mut self, message: &TaskMessage) -> Result<(), CompleteError> {
            self.completed.push(message.id.clone());
            Ok(())
        }
    }

    impl RetryBroker for RecordingBroker {
        fn retry(
            &mut self,
            message: &TaskMessage,
            retry_at: SystemTime,
            error_message: &str,
            is_failure: bool,
        ) -> Result<(), RetryError> {
            self.retried.push((
                message.id.clone(),
                retry_at,
                error_message.to_owned(),
                is_failure,
            ));
            Ok(())
        }
    }

    impl ArchiveBroker for RecordingBroker {
        fn archive(
            &mut self,
            message: &TaskMessage,
            archived_at: SystemTime,
            error_message: &str,
            is_failure: bool,
        ) -> Result<(), ArchiveError> {
            self.archived.push((
                message.id.clone(),
                archived_at,
                error_message.to_owned(),
                is_failure,
            ));
            Ok(())
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct TestClock(SystemTime);

    impl Clock for TestClock {
        fn now(&self) -> SystemTime {
            self.0
        }
    }

    fn dequeued_message(retried: i32, retry: i32) -> DequeuedTask {
        dequeued_message_with_retention(retried, retry, 0)
    }

    fn dequeued_message_with_retention(retried: i32, retry: i32, retention: i64) -> DequeuedTask {
        let mut message = TaskMessage::from_task(&Task::with_headers(
            "email:welcome",
            b"payload".to_vec(),
            [("trace-id", "abc")],
        ));
        message.id = "task-id".to_owned();
        message.queue = "critical".to_owned();
        message.retried = retried;
        message.retry = retry;
        message.retention = retention;
        DequeuedTask::new(message, SystemTime::UNIX_EPOCH + Duration::from_secs(30))
    }

    #[test]
    fn completes_successful_task() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let broker = RecordingBroker {
            dequeued: vec![Ok(dequeued_message(0, 3))],
            ..RecordingBroker::default()
        };
        let mut processor = Processor::with_parts(
            broker,
            |task: &Task| {
                assert_eq!(task.type_name(), "email:welcome");
                assert_eq!(task.payload(), b"payload");
                assert_eq!(task.header("trace-id"), Some("abc"));
                Ok(())
            },
            |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
            TestClock(now),
        );

        let result = processor.run_once(&["critical".to_owned()]).unwrap();

        assert_eq!(
            result,
            ProcessorRun::Completed {
                task_id: "task-id".to_owned()
            }
        );
        assert_eq!(processor.broker().completed, ["task-id"]);
    }

    #[test]
    fn retries_failed_task_with_configured_delay() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let broker = RecordingBroker {
            dequeued: vec![Ok(dequeued_message(1, 3))],
            ..RecordingBroker::default()
        };
        let mut processor = Processor::with_parts(
            broker,
            |_task: &Task| Err(HandlerError::failed("handler failed")),
            |retried, error: &HandlerError, task: &Task| {
                assert_eq!(retried, 1);
                assert_eq!(error.to_string(), "handler failed");
                assert_eq!(task.type_name(), "email:welcome");
                Duration::from_secs(45)
            },
            TestClock(now),
        );

        let result = processor.run_once(&["critical".to_owned()]).unwrap();

        assert_eq!(
            result,
            ProcessorRun::Retried {
                task_id: "task-id".to_owned(),
                retry_at: now + Duration::from_secs(45),
            }
        );
        assert_eq!(
            processor.broker().retried,
            [(
                "task-id".to_owned(),
                now + Duration::from_secs(45),
                "handler failed".to_owned(),
                true
            )]
        );
    }

    #[test]
    fn custom_is_failure_controls_retry_failure_flag_and_error_handler_runs() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let broker = RecordingBroker {
            dequeued: vec![Ok(dequeued_message(1, 3))],
            ..RecordingBroker::default()
        };
        let mut processor = Processor::with_parts(
            broker,
            |_task: &Task| Err(HandlerError::failed("transient")),
            |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(45),
            TestClock(now),
        )
        .with_is_failure(|error: &HandlerError| error.to_string() != "transient")
        .with_error_handler(VecErrorHandler::default());

        let result = processor.run_once(&["critical".to_owned()]).unwrap();

        assert_eq!(
            result,
            ProcessorRun::Retried {
                task_id: "task-id".to_owned(),
                retry_at: now + Duration::from_secs(45),
            }
        );
        assert_eq!(
            processor.broker().retried,
            [(
                "task-id".to_owned(),
                now + Duration::from_secs(45),
                "transient".to_owned(),
                false
            )]
        );
        assert_eq!(
            processor.error_handler().errors,
            [("email:welcome".to_owned(), "transient".to_owned())]
        );
    }

    #[test]
    fn error_handler_runs_before_archive_and_revoke_paths() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let broker = RecordingBroker {
            dequeued: vec![Ok(dequeued_message(3, 3)), Ok(dequeued_message(0, 3))],
            ..RecordingBroker::default()
        };
        let mut attempts = 0;
        let mut processor = Processor::with_parts(
            broker,
            move |_task: &Task| {
                attempts += 1;
                if attempts == 1 {
                    Err(HandlerError::failed("exhausted"))
                } else {
                    Err(HandlerError::revoke_task("revoke"))
                }
            },
            |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
            TestClock(now),
        )
        .with_error_handler(VecErrorHandler::default());

        let first = processor.run_once(&["critical".to_owned()]).unwrap();
        let second = processor.run_once(&["critical".to_owned()]).unwrap();

        assert_eq!(
            first,
            ProcessorRun::Archived {
                task_id: "task-id".to_owned()
            }
        );
        assert_eq!(
            second,
            ProcessorRun::Revoked {
                task_id: "task-id".to_owned()
            }
        );
        assert_eq!(
            processor.error_handler().errors,
            [
                ("email:welcome".to_owned(), "exhausted".to_owned()),
                ("email:welcome".to_owned(), "revoke".to_owned()),
            ]
        );
    }

    #[test]
    fn archives_when_retry_is_exhausted_or_skipped() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let broker = RecordingBroker {
            dequeued: vec![Ok(dequeued_message(3, 3)), Ok(dequeued_message(0, 3))],
            ..RecordingBroker::default()
        };
        let mut attempts = 0;
        let mut processor = Processor::with_parts(
            broker,
            move |_task: &Task| {
                attempts += 1;
                if attempts == 1 {
                    Err(HandlerError::failed("exhausted"))
                } else {
                    Err(HandlerError::skip_retry("skip retry"))
                }
            },
            |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
            TestClock(now),
        );

        let first = processor.run_once(&["critical".to_owned()]).unwrap();
        let second = processor.run_once(&["critical".to_owned()]).unwrap();

        assert_eq!(
            first,
            ProcessorRun::Archived {
                task_id: "task-id".to_owned()
            }
        );
        assert_eq!(
            second,
            ProcessorRun::Archived {
                task_id: "task-id".to_owned()
            }
        );
        assert_eq!(
            processor.broker().archived,
            [
                ("task-id".to_owned(), now, "exhausted".to_owned(), true),
                ("task-id".to_owned(), now, "skip retry".to_owned(), true),
            ]
        );
    }

    #[test]
    fn revoke_task_marks_done_without_retry_or_archive() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let broker = RecordingBroker {
            dequeued: vec![Ok(dequeued_message_with_retention(0, 3, 300))],
            ..RecordingBroker::default()
        };
        let mut processor = Processor::with_parts(
            broker,
            |_task: &Task| Err(HandlerError::revoke_task("revoke")),
            |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
            TestClock(now),
        );

        let result = processor.run_once(&["critical".to_owned()]).unwrap();

        assert_eq!(
            result,
            ProcessorRun::Revoked {
                task_id: "task-id".to_owned()
            }
        );
        assert_eq!(processor.broker().completed, ["task-id"]);
        assert!(processor.broker().retried.is_empty());
        assert!(processor.broker().archived.is_empty());
    }

    #[test]
    fn reports_idle_without_error() {
        let broker = RecordingBroker {
            dequeued: vec![Err(DequeueError::NoProcessableTask)],
            ..RecordingBroker::default()
        };
        let mut processor = Processor::with_parts(
            broker,
            |_task: &Task| Ok(()),
            |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
            TestClock(SystemTime::UNIX_EPOCH),
        );

        let result = processor.run_once(&["critical".to_owned()]).unwrap();

        assert_eq!(result, ProcessorRun::NoProcessableTask);
    }

    #[test]
    fn handler_panic_is_retried_like_handler_failure() {
        let broker = RecordingBroker {
            dequeued: vec![Ok(dequeued_message(0, 3))],
            ..RecordingBroker::default()
        };
        let mut processor = Processor::with_parts(
            broker,
            |_task: &Task| -> Result<(), HandlerError> { panic!("boom") },
            |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
            TestClock(SystemTime::UNIX_EPOCH),
        );

        let result = processor.run_once(&["critical".to_owned()]).unwrap();

        assert_eq!(
            result,
            ProcessorRun::Retried {
                task_id: "task-id".to_owned(),
                retry_at: SystemTime::UNIX_EPOCH + Duration::from_secs(60),
            }
        );
        assert!(processor.broker().completed.is_empty());
        assert_eq!(
            processor.broker().retried,
            [(
                "task-id".to_owned(),
                SystemTime::UNIX_EPOCH + Duration::from_secs(60),
                "boom".to_owned(),
                true
            )]
        );
        assert!(processor.broker().archived.is_empty());
    }

    #[test]
    fn default_retry_delay_uses_expected_bounds() {
        let delay = DefaultRetryDelay::delay_for_retried_count(2);

        assert!(delay >= Duration::from_secs(31));
        assert!(delay <= Duration::from_secs(118));
    }

    #[derive(Debug, Default)]
    struct VecErrorHandler {
        errors: Vec<(String, String)>,
    }

    impl ErrorHandler for VecErrorHandler {
        fn handle_error(&mut self, task: &Task, error: &HandlerError) {
            self.errors
                .push((task.type_name().to_owned(), error.to_string()));
        }
    }
}
