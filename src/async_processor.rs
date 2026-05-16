use std::panic::{AssertUnwindSafe, catch_unwind};
use std::task::Poll;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use thiserror::Error;

use crate::{
    ArchiveError, AsyncWorkerProcessor, CompleteError, DEFAULT_ASYNC_SERVER_RECOVER_RETRY_DELAY,
    DequeueError, DequeuedTask, ForwardError, LeaseError, RecoverError, RecoverResult,
    RequeueError, RetryError, ServerMaintenanceRun, SystemClock, Task, TaskMessage,
};

/// Async minimal broker interface for the worker dequeue path.
///
/// Reference: Asynq v0.26.0 internal `base.Broker` worker-related methods:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L371-L419>.
#[async_trait]
pub trait AsyncDequeueBroker {
    async fn dequeue(&mut self, queues: &[String]) -> Result<DequeuedTask, DequeueError>;
}

/// Async broker interface for marking a dequeued task as successfully done.
///
/// Reference: Asynq v0.26.0 `RDB.Done` and `RDB.MarkAsComplete`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L325-L379>.
#[async_trait]
pub trait AsyncCompleteBroker {
    async fn complete(&mut self, message: &TaskMessage) -> Result<(), CompleteError>;
}

/// Async broker interface for retrying a failed active task.
///
/// Reference: Asynq v0.26.0 `RDB.Retry`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L380-L418>.
#[async_trait]
pub trait AsyncRetryBroker {
    async fn retry(
        &mut self,
        message: &TaskMessage,
        retry_at: SystemTime,
        error_message: &str,
        is_failure: bool,
    ) -> Result<(), RetryError>;
}

/// Async broker interface for archiving a failed active task.
///
/// Reference: Asynq v0.26.0 archive-related task state handling in
/// `internal/rdb`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.
#[async_trait]
pub trait AsyncArchiveBroker {
    async fn archive(
        &mut self,
        message: &TaskMessage,
        archived_at: SystemTime,
        error_message: &str,
        is_failure: bool,
    ) -> Result<(), ArchiveError>;
}

/// Async broker interface for extending active task leases.
///
/// Reference: Asynq v0.26.0 `RDB.ExtendLease` updates active-task lease scores:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.
#[async_trait]
pub trait AsyncLeaseBroker {
    async fn extend_lease(&mut self, queue: &str, task_id: &str) -> Result<(), LeaseError>;
}

/// Async broker interface for moving due scheduled/retry tasks back to pending.
///
/// Reference: Asynq v0.26.0 `RDB.ForwardIfReady` and `forwardCmd`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L861-L900>.
#[async_trait]
pub trait AsyncForwardBroker {
    async fn forward_scheduled(&mut self, queue: &str) -> Result<usize, ForwardError>;

    async fn forward_retry(&mut self, queue: &str) -> Result<usize, ForwardError>;
}

/// Async broker interface for recovering active tasks whose leases expired.
///
/// Reference: Asynq v0.26.0 recoverer lists lease-expired tasks and sends them
/// through retry/archive lifecycle paths:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/recoverer.go>.
#[async_trait]
pub trait AsyncRecoverBroker {
    async fn recover_expired_leases(
        &mut self,
        queue: &str,
        retry_at: SystemTime,
        error_message: &str,
    ) -> Result<RecoverResult, RecoverError>;
}

/// Async broker interface for moving an active task back to pending during
/// worker shutdown.
///
/// Reference: Asynq v0.26.0 `RDB.Requeue`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L486-L506>.
#[async_trait]
pub trait AsyncRequeueBroker {
    async fn requeue(&mut self, message: &TaskMessage) -> Result<(), RequeueError>;
}

/// Error returned by a task handler.
///
/// Reference: Asynq v0.26.0 `SkipRetry` and `RevokeTask` handler sentinel
/// errors:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L327-L348>.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum HandlerError {
    #[error("{0}")]
    Failed(String),
    #[error("{0}")]
    SkipRetry(String),
    #[error("{0}")]
    RevokeTask(String),
    #[error("{0}")]
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

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProcessorError {
    #[error("failed to dequeue task: {0}")]
    Dequeue(#[from] DequeueError),
    #[error("failed to complete task: {0}")]
    Complete(#[from] CompleteError),
    #[error("failed to retry task: {0}")]
    Retry(#[from] RetryError),
    #[error("failed to archive task: {0}")]
    Archive(#[from] ArchiveError),
    #[error("failed to extend task lease: {0}")]
    Lease(#[from] LeaseError),
    #[error("failed to forward ready tasks: {0}")]
    Forward(#[from] ForwardError),
    #[error("failed to recover expired leases: {0}")]
    Recover(#[from] RecoverError),
    #[error("failed to requeue active task: {0}")]
    Requeue(#[from] RequeueError),
    #[error("{0} overflowed")]
    TimeOverflow(&'static str),
}

/// Processes a single task on an async runtime.
///
/// Reference: Asynq v0.26.0 public `Handler` and `HandlerFunc` APIs:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L622-L650>.
#[async_trait]
pub trait AsyncHandler {
    async fn process_task(&mut self, task: &Task) -> Result<(), HandlerError>;
}

#[async_trait]
impl<F> AsyncHandler for F
where
    F: FnMut(&Task) -> Result<(), HandlerError> + Send,
{
    async fn process_task(&mut self, task: &Task) -> Result<(), HandlerError> {
        self(task)
    }
}

/// Handles errors returned by async task handlers.
///
/// Reference: Asynq v0.26.0 `ErrorHandler` and processor error hook:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L277-L287>,
/// <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L335-L338>.
#[async_trait]
pub trait AsyncErrorHandler {
    async fn handle_error(&mut self, task: &Task, error: &HandlerError);
}

#[async_trait]
impl AsyncErrorHandler for NoopErrorHandler {
    async fn handle_error(&mut self, _task: &Task, _error: &HandlerError) {}
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopAsyncLeaseExtender;

#[derive(Debug, Clone, Copy, Default)]
pub struct AsyncExtendLeaseBeforeProcess;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AsyncExtendLeaseWhileProcessing {
    interval: Duration,
}

/// Extends or starts lease extension for a dequeued task before handler
/// execution.
///
/// Reference: Asynq v0.26.0 starts a lease extender goroutine around task
/// processing:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L221-L381>.
#[async_trait]
pub trait AsyncLeaseExtender<B> {
    async fn before_process(
        &mut self,
        broker: &mut B,
        message: &TaskMessage,
    ) -> Result<(), LeaseError>;

    fn during_process_interval(&self) -> Option<Duration> {
        None
    }

    async fn during_process(
        &mut self,
        _broker: &mut B,
        _message: &TaskMessage,
    ) -> Result<(), LeaseError> {
        Ok(())
    }
}

#[async_trait]
impl<B> AsyncLeaseExtender<B> for NoopAsyncLeaseExtender
where
    B: Send,
{
    async fn before_process(
        &mut self,
        _broker: &mut B,
        _message: &TaskMessage,
    ) -> Result<(), LeaseError> {
        Ok(())
    }
}

#[async_trait]
impl<B> AsyncLeaseExtender<B> for AsyncExtendLeaseBeforeProcess
where
    B: AsyncLeaseBroker + Send,
{
    async fn before_process(
        &mut self,
        broker: &mut B,
        message: &TaskMessage,
    ) -> Result<(), LeaseError> {
        broker.extend_lease(&message.queue, &message.id).await
    }
}

impl AsyncExtendLeaseWhileProcessing {
    pub fn every(interval: Duration) -> Self {
        Self { interval }
    }

    pub fn interval(&self) -> Duration {
        self.interval
    }
}

#[async_trait]
impl<B> AsyncLeaseExtender<B> for AsyncExtendLeaseWhileProcessing
where
    B: AsyncLeaseBroker + Send,
{
    async fn before_process(
        &mut self,
        _broker: &mut B,
        _message: &TaskMessage,
    ) -> Result<(), LeaseError> {
        Ok(())
    }

    fn during_process_interval(&self) -> Option<Duration> {
        if self.interval.is_zero() {
            None
        } else {
            Some(self.interval)
        }
    }

    async fn during_process(
        &mut self,
        broker: &mut B,
        message: &TaskMessage,
    ) -> Result<(), LeaseError> {
        broker.extend_lease(&message.queue, &message.id).await
    }
}

/// Minimal async worker-side processor that runs one dequeued task through a
/// handler and then marks it complete, retry, archive, or done.
///
/// Reference: Asynq v0.26.0 processor success/failure routing:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L221-L381>.
///
#[derive(Debug, Clone)]
pub struct AsyncProcessor<
    B,
    H,
    R = DefaultRetryDelay,
    C = SystemClock,
    I = DefaultIsFailure,
    E = NoopErrorHandler,
    L = NoopAsyncLeaseExtender,
> {
    broker: B,
    handler: H,
    retry_delay: R,
    clock: C,
    is_failure: I,
    error_handler: E,
    lease_extender: L,
    active_message: Option<TaskMessage>,
}

impl<B, H> AsyncProcessor<B, H, DefaultRetryDelay, SystemClock> {
    pub fn new(broker: B, handler: H) -> Self {
        Self::with_parts(broker, handler, DefaultRetryDelay, SystemClock)
    }
}

impl<B, H, R> AsyncProcessor<B, H, R, SystemClock> {
    pub fn with_retry_delay(broker: B, handler: H, retry_delay: R) -> Self {
        Self::with_parts(broker, handler, retry_delay, SystemClock)
    }
}

impl<B, H, R, C> AsyncProcessor<B, H, R, C> {
    pub fn with_parts(broker: B, handler: H, retry_delay: R, clock: C) -> Self {
        Self::with_parts_hooks_and_lease_extender(
            broker,
            handler,
            retry_delay,
            clock,
            DefaultIsFailure,
            NoopErrorHandler,
            NoopAsyncLeaseExtender,
        )
    }
}

impl<B, H, R, C, I, E, L> AsyncProcessor<B, H, R, C, I, E, L> {
    pub fn with_parts_hooks_and_lease_extender(
        broker: B,
        handler: H,
        retry_delay: R,
        clock: C,
        is_failure: I,
        error_handler: E,
        lease_extender: L,
    ) -> Self {
        Self {
            broker,
            handler,
            retry_delay,
            clock,
            is_failure,
            error_handler,
            lease_extender,
            active_message: None,
        }
    }

    pub fn with_is_failure<I2>(self, is_failure: I2) -> AsyncProcessor<B, H, R, C, I2, E, L> {
        AsyncProcessor {
            broker: self.broker,
            handler: self.handler,
            retry_delay: self.retry_delay,
            clock: self.clock,
            is_failure,
            error_handler: self.error_handler,
            lease_extender: self.lease_extender,
            active_message: self.active_message,
        }
    }

    pub fn with_error_handler<E2>(self, error_handler: E2) -> AsyncProcessor<B, H, R, C, I, E2, L> {
        AsyncProcessor {
            broker: self.broker,
            handler: self.handler,
            retry_delay: self.retry_delay,
            clock: self.clock,
            is_failure: self.is_failure,
            error_handler,
            lease_extender: self.lease_extender,
            active_message: self.active_message,
        }
    }

    pub fn with_lease_extender<L2>(
        self,
        lease_extender: L2,
    ) -> AsyncProcessor<B, H, R, C, I, E, L2> {
        AsyncProcessor {
            broker: self.broker,
            handler: self.handler,
            retry_delay: self.retry_delay,
            clock: self.clock,
            is_failure: self.is_failure,
            error_handler: self.error_handler,
            lease_extender,
            active_message: self.active_message,
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

impl<B, H, R, C, I, E, L> AsyncProcessor<B, H, R, C, I, E, L>
where
    B: AsyncDequeueBroker + AsyncCompleteBroker + AsyncRetryBroker + AsyncArchiveBroker + Send,
    H: AsyncHandler + Send,
    R: RetryDelay + Send,
    C: crate::Clock + Send + Sync,
    I: IsFailure + Send,
    E: AsyncErrorHandler + Send,
    L: AsyncLeaseExtender<B> + Send,
{
    pub async fn run_once(&mut self, queues: &[String]) -> Result<ProcessorRun, ProcessorError> {
        let dequeued = match self.broker.dequeue(queues).await {
            Ok(task) => task,
            Err(DequeueError::NoProcessableTask) => return Ok(ProcessorRun::NoProcessableTask),
            Err(error) => return Err(ProcessorError::Dequeue(error)),
        };

        let message = dequeued.message().clone();
        let task = task_from_message(&message);
        let now = self.clock.now();
        let deadline = task_deadline(&message, now)?;
        self.active_message = Some(message.clone());
        if let Err(error) = self
            .lease_extender
            .before_process(&mut self.broker, &message)
            .await
        {
            self.active_message = None;
            return Err(error.into());
        }
        let handler_result = perform_with_lease_extender(
            &mut self.handler,
            &mut self.lease_extender,
            &mut self.broker,
            &message,
            &task,
            deadline,
            now,
        )
        .await?;
        let result = match handler_result {
            Ok(()) => match self.broker.complete(&message).await {
                Ok(()) => Ok(ProcessorRun::Completed {
                    task_id: message.id,
                }),
                Err(error) => Err(error.into()),
            },
            Err(error) => self.handle_failure(message, task, error).await,
        };
        self.active_message = None;
        result
    }

    async fn handle_failure(
        &mut self,
        message: TaskMessage,
        task: Task,
        error: HandlerError,
    ) -> Result<ProcessorRun, ProcessorError> {
        self.error_handler.handle_error(&task, &error).await;
        match error {
            HandlerError::RevokeTask(_) => {
                let mut revoked = message.clone();
                revoked.retention = 0;
                self.broker.complete(&revoked).await?;
                Ok(ProcessorRun::Revoked {
                    task_id: message.id,
                })
            }
            error @ HandlerError::SkipRetry(_) => {
                let error_message = error.to_string();
                self.broker
                    .archive(&message, self.clock.now(), &error_message, true)
                    .await?;
                Ok(ProcessorRun::Archived {
                    task_id: message.id,
                })
            }
            error if message.retried >= message.retry => {
                let error_message = error.to_string();
                self.broker
                    .archive(&message, self.clock.now(), &error_message, true)
                    .await?;
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
                    .retry(&message, retry_at, &error_message, is_failure)
                    .await?;
                Ok(ProcessorRun::Retried {
                    task_id: message.id,
                    retry_at,
                })
            }
        }
    }

    pub async fn run_until_idle(&mut self, queues: &[String]) -> Result<usize, ProcessorError> {
        let mut processed = 0;
        loop {
            match self.run_once(queues).await? {
                ProcessorRun::NoProcessableTask => return Ok(processed),
                _ => processed += 1,
            }
        }
    }
}

impl<B, H, R, C, I, E, L> AsyncProcessor<B, H, R, C, I, E, L>
where
    B: AsyncRequeueBroker + Send,
{
    /// Requeues the task currently owned by this worker, if any.
    ///
    /// Reference: Asynq v0.26.0 worker shutdown requeues active tasks through
    /// `RDB.Requeue`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L486-L506>.
    pub async fn shutdown(&mut self) -> Result<(), ProcessorError> {
        if let Some(message) = self.active_message.take() {
            self.broker.requeue(&message).await?;
        }
        Ok(())
    }
}

impl<B, H, R, C, I, E, L> AsyncProcessor<B, H, R, C, I, E, L>
where
    B: AsyncForwardBroker + AsyncRecoverBroker + Send,
    C: crate::Clock + Send + Sync,
{
    /// Runs one async server-maintenance pass.
    ///
    /// Reference: Asynq v0.26.0 starts background forwarder and recoverer
    /// components from `Server.Start`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L687-L695>.
    ///
    /// TODO: Replace the fixed retry delay with upstream retry-delay
    /// calculation and separate polling intervals when async server runtime is
    /// modeled.
    pub async fn run_maintenance(
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
            .checked_add(DEFAULT_ASYNC_SERVER_RECOVER_RETRY_DELAY)
            .ok_or(ProcessorError::TimeOverflow("recovery retry time"))?;

        for queue in queues {
            forwarded_scheduled += self.broker.forward_scheduled(queue).await?;
            forwarded_retry += self.broker.forward_retry(queue).await?;
            let recovered = self
                .broker
                .recover_expired_leases(queue, retry_at, "task lease expired")
                .await?;
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

#[async_trait]
impl<B, H, R, C, I, E, L> AsyncWorkerProcessor for AsyncProcessor<B, H, R, C, I, E, L>
where
    B: AsyncDequeueBroker
        + AsyncCompleteBroker
        + AsyncRetryBroker
        + AsyncArchiveBroker
        + AsyncRequeueBroker
        + AsyncForwardBroker
        + AsyncRecoverBroker
        + Send,
    H: AsyncHandler + Send,
    R: RetryDelay + Send,
    C: crate::Clock + Send + Sync,
    I: IsFailure + Send,
    E: AsyncErrorHandler + Send,
    L: AsyncLeaseExtender<B> + Send,
{
    async fn run_once(&mut self, queues: &[String]) -> Result<ProcessorRun, ProcessorError> {
        AsyncProcessor::run_once(self, queues).await
    }

    async fn run_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessorError> {
        AsyncProcessor::run_maintenance(self, queues).await
    }

    async fn shutdown(&mut self) -> Result<(), ProcessorError> {
        AsyncProcessor::shutdown(self).await
    }
}

fn task_from_message(message: &TaskMessage) -> Task {
    Task::with_headers(
        message.r#type.clone(),
        message.payload.clone(),
        message.headers.clone(),
    )
}

async fn perform<H>(
    handler: &mut H,
    task: &Task,
    deadline: Option<SystemTime>,
    now: SystemTime,
) -> Result<(), HandlerError>
where
    H: AsyncHandler,
{
    if deadline.is_some_and(|deadline| deadline <= now) {
        return Err(HandlerError::failed("context deadline exceeded"));
    }

    let future = handler.process_task(task);
    tokio::pin!(future);
    let caught = std::future::poll_fn(|cx| {
        catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))).unwrap_or_else(|panic| {
            let message = if let Some(message) = panic.downcast_ref::<&str>() {
                (*message).to_owned()
            } else if let Some(message) = panic.downcast_ref::<String>() {
                message.clone()
            } else {
                "task handler panicked".to_owned()
            };
            Poll::Ready(Err(HandlerError::Panic(message)))
        })
    });

    match deadline.and_then(|deadline| tokio_instant_for_system_time(deadline, now)) {
        Some(deadline) => tokio::time::timeout_at(deadline, caught)
            .await
            .unwrap_or_else(|_| Err(HandlerError::failed("context deadline exceeded"))),
        None => caught.await,
    }
}

async fn perform_with_lease_extender<H, L, B>(
    handler: &mut H,
    lease_extender: &mut L,
    broker: &mut B,
    message: &TaskMessage,
    task: &Task,
    deadline: Option<SystemTime>,
    now: SystemTime,
) -> Result<Result<(), HandlerError>, ProcessorError>
where
    H: AsyncHandler + Send,
    L: AsyncLeaseExtender<B> + Send,
    B: Send,
{
    let Some(interval) = lease_extender.during_process_interval() else {
        return Ok(perform(handler, task, deadline, now).await);
    };

    if deadline.is_some_and(|deadline| deadline <= now) {
        return Ok(Err(HandlerError::failed("context deadline exceeded")));
    }

    let handler = perform(handler, task, deadline, now);
    tokio::pin!(handler);
    let mut lease_interval = tokio::time::interval(interval);
    lease_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            result = &mut handler => return Ok(result),
            _ = lease_interval.tick() => {
                lease_extender.during_process(broker, message).await?;
            }
        }
    }
}

fn task_deadline(
    message: &TaskMessage,
    now: SystemTime,
) -> Result<Option<SystemTime>, ProcessorError> {
    let timeout_deadline = if message.timeout > 0 {
        let timeout = Duration::from_secs(message.timeout as u64);
        Some(
            now.checked_add(timeout)
                .ok_or(ProcessorError::TimeOverflow("task timeout deadline"))?,
        )
    } else {
        None
    };
    let explicit_deadline = if message.deadline != 0 {
        Some(system_time_from_unix_seconds(message.deadline))
    } else {
        None
    };

    Ok(match (timeout_deadline, explicit_deadline) {
        (Some(timeout), Some(deadline)) => Some(timeout.min(deadline)),
        (Some(timeout), None) => Some(timeout),
        (None, Some(deadline)) => Some(deadline),
        (None, None) => None,
    })
}

fn system_time_from_unix_seconds(seconds: i64) -> SystemTime {
    if seconds >= 0 {
        UNIX_EPOCH + Duration::from_secs(seconds as u64)
    } else {
        UNIX_EPOCH - Duration::from_secs(seconds.unsigned_abs())
    }
}

fn tokio_instant_for_system_time(
    deadline: SystemTime,
    now: SystemTime,
) -> Option<tokio::time::Instant> {
    match deadline.duration_since(now) {
        Ok(duration) => tokio::time::Instant::now().checked_add(duration),
        Err(_) => Some(tokio::time::Instant::now()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    use crate::{Clock, DefaultIsFailure, DefaultRetryDelay};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[derive(Debug, Default)]
    struct RecordingAsyncBroker {
        dequeued: Vec<Result<DequeuedTask, DequeueError>>,
        completed: Vec<String>,
        retried: Vec<(String, SystemTime, String, bool)>,
        archived: Vec<(String, SystemTime, String, bool)>,
        requeued: Vec<String>,
        lease_extensions: Vec<(String, String)>,
        extend_lease_error: Option<LeaseError>,
        forward_scheduled: usize,
        forward_retry: usize,
        recover_result: RecoverResult,
    }

    #[async_trait]
    impl AsyncDequeueBroker for RecordingAsyncBroker {
        async fn dequeue(&mut self, _queues: &[String]) -> Result<DequeuedTask, DequeueError> {
            self.dequeued.remove(0)
        }
    }

    #[async_trait]
    impl AsyncCompleteBroker for RecordingAsyncBroker {
        async fn complete(&mut self, message: &TaskMessage) -> Result<(), CompleteError> {
            self.completed.push(message.id.clone());
            Ok(())
        }
    }

    #[async_trait]
    impl AsyncRetryBroker for RecordingAsyncBroker {
        async fn retry(
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

    #[async_trait]
    impl AsyncArchiveBroker for RecordingAsyncBroker {
        async fn archive(
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

    #[async_trait]
    impl AsyncLeaseBroker for RecordingAsyncBroker {
        async fn extend_lease(&mut self, queue: &str, task_id: &str) -> Result<(), LeaseError> {
            self.lease_extensions
                .push((queue.to_owned(), task_id.to_owned()));
            if let Some(error) = self.extend_lease_error.clone() {
                return Err(error);
            }
            Ok(())
        }
    }

    #[async_trait]
    impl AsyncRequeueBroker for RecordingAsyncBroker {
        async fn requeue(&mut self, message: &TaskMessage) -> Result<(), RequeueError> {
            self.requeued.push(message.id.clone());
            Ok(())
        }
    }

    #[async_trait]
    impl AsyncForwardBroker for RecordingAsyncBroker {
        async fn forward_scheduled(&mut self, _queue: &str) -> Result<usize, ForwardError> {
            Ok(self.forward_scheduled)
        }

        async fn forward_retry(&mut self, _queue: &str) -> Result<usize, ForwardError> {
            Ok(self.forward_retry)
        }
    }

    #[async_trait]
    impl AsyncRecoverBroker for RecordingAsyncBroker {
        async fn recover_expired_leases(
            &mut self,
            _queue: &str,
            _retry_at: SystemTime,
            _error_message: &str,
        ) -> Result<RecoverResult, RecoverError> {
            Ok(self.recover_result)
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct TestClock(SystemTime);

    impl Clock for TestClock {
        fn now(&self) -> SystemTime {
            self.0
        }
    }

    fn message(id: &str) -> TaskMessage {
        let mut message = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
        message.id = id.to_owned();
        message.queue = "critical".to_owned();
        message.retry = 3;
        message
    }

    fn dequeued(message: TaskMessage) -> DequeuedTask {
        DequeuedTask::new(message, UNIX_EPOCH + Duration::from_secs(30))
    }

    #[tokio::test]
    async fn completes_successful_task() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let message = message("task-id");
        let broker = RecordingAsyncBroker {
            dequeued: vec![Ok(dequeued(message))],
            ..RecordingAsyncBroker::default()
        };
        let mut processor = AsyncProcessor::with_parts(
            broker,
            |_task: &Task| Ok::<(), HandlerError>(()),
            DefaultRetryDelay,
            TestClock(now),
        );

        let result = processor.run_once(&["critical".to_owned()]).await.unwrap();

        assert_eq!(
            result,
            ProcessorRun::Completed {
                task_id: "task-id".to_owned()
            }
        );
        assert_eq!(processor.broker().completed, ["task-id"]);
    }

    #[tokio::test]
    async fn retries_failed_task_with_configured_delay() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let message = message("task-id");
        let broker = RecordingAsyncBroker {
            dequeued: vec![Ok(dequeued(message))],
            ..RecordingAsyncBroker::default()
        };
        let mut processor = AsyncProcessor::with_parts(
            broker,
            |_task: &Task| Err(HandlerError::failed("boom")),
            |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
            TestClock(now),
        );

        let result = processor.run_once(&["critical".to_owned()]).await.unwrap();

        assert_eq!(
            result,
            ProcessorRun::Retried {
                task_id: "task-id".to_owned(),
                retry_at: now + Duration::from_secs(60)
            }
        );
        assert_eq!(
            processor.broker().retried,
            [(
                "task-id".to_owned(),
                now + Duration::from_secs(60),
                "boom".to_owned(),
                true
            )]
        );
    }

    #[tokio::test]
    async fn archives_when_retry_is_exhausted_or_skipped() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let mut exhausted = message("exhausted-id");
        exhausted.retry = 1;
        exhausted.retried = 1;
        let skipped = message("skipped-id");
        let broker = RecordingAsyncBroker {
            dequeued: vec![Ok(dequeued(exhausted)), Ok(dequeued(skipped))],
            ..RecordingAsyncBroker::default()
        };
        let mut processor = AsyncProcessor::with_parts(
            broker,
            {
                let mut calls = 0;
                move |_task: &Task| {
                    calls += 1;
                    if calls == 1 {
                        Err(HandlerError::failed("exhausted"))
                    } else {
                        Err(HandlerError::skip_retry("skip"))
                    }
                }
            },
            |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
            TestClock(now),
        );

        let first = processor.run_once(&["critical".to_owned()]).await.unwrap();
        let second = processor.run_once(&["critical".to_owned()]).await.unwrap();

        assert_eq!(
            first,
            ProcessorRun::Archived {
                task_id: "exhausted-id".to_owned()
            }
        );
        assert_eq!(
            second,
            ProcessorRun::Archived {
                task_id: "skipped-id".to_owned()
            }
        );
        assert_eq!(processor.broker().archived.len(), 2);
    }

    #[tokio::test]
    async fn lease_extension_error_stops_processing() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let message = message("task-id");
        let broker = RecordingAsyncBroker {
            dequeued: vec![Ok(dequeued(message))],
            extend_lease_error: Some(LeaseError::Other("lease failed".to_owned())),
            ..RecordingAsyncBroker::default()
        };
        let mut processor = AsyncProcessor::with_parts_hooks_and_lease_extender(
            broker,
            |_task: &Task| Ok::<(), HandlerError>(()),
            DefaultRetryDelay,
            TestClock(now),
            DefaultIsFailure,
            NoopErrorHandler,
            AsyncExtendLeaseBeforeProcess,
        );

        let error = processor
            .run_once(&["critical".to_owned()])
            .await
            .unwrap_err();

        assert_eq!(
            error,
            ProcessorError::Lease(LeaseError::Other("lease failed".to_owned()))
        );
        assert!(processor.broker().completed.is_empty());
        assert_eq!(
            processor.broker().lease_extensions,
            [("critical".to_owned(), "task-id".to_owned())]
        );
    }

    #[tokio::test]
    async fn background_lease_extender_runs_while_handler_is_processing() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let message = message("task-id");
        let broker = RecordingAsyncBroker {
            dequeued: vec![Ok(dequeued(message))],
            ..RecordingAsyncBroker::default()
        };
        let mut processor = AsyncProcessor::with_parts_hooks_and_lease_extender(
            broker,
            SleepingAsyncHandler {
                duration: Duration::from_millis(20),
            },
            DefaultRetryDelay,
            TestClock(now),
            DefaultIsFailure,
            NoopErrorHandler,
            AsyncExtendLeaseWhileProcessing::every(Duration::from_millis(5)),
        );

        let result = processor.run_once(&["critical".to_owned()]).await.unwrap();

        assert_eq!(
            result,
            ProcessorRun::Completed {
                task_id: "task-id".to_owned()
            }
        );
        assert!(processor.broker().lease_extensions.len() >= 2);
        assert!(
            processor
                .broker()
                .lease_extensions
                .iter()
                .all(|extension| extension == &("critical".to_owned(), "task-id".to_owned()))
        );
    }

    #[tokio::test]
    async fn background_lease_extension_stops_after_handler_returns() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let message = message("task-id");
        let broker = RecordingAsyncBroker {
            dequeued: vec![Ok(dequeued(message))],
            ..RecordingAsyncBroker::default()
        };
        let mut processor = AsyncProcessor::with_parts_hooks_and_lease_extender(
            broker,
            |_task: &Task| Ok::<(), HandlerError>(()),
            DefaultRetryDelay,
            TestClock(now),
            DefaultIsFailure,
            NoopErrorHandler,
            AsyncExtendLeaseWhileProcessing::every(Duration::from_millis(5)),
        );

        let result = processor.run_once(&["critical".to_owned()]).await.unwrap();
        let extensions_after_return = processor.broker().lease_extensions.len();
        tokio::time::sleep(Duration::from_millis(15)).await;

        assert_eq!(
            result,
            ProcessorRun::Completed {
                task_id: "task-id".to_owned()
            }
        );
        assert_eq!(
            processor.broker().lease_extensions.len(),
            extensions_after_return
        );
    }

    #[tokio::test]
    async fn background_lease_extension_error_stops_processing() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let message = message("task-id");
        let broker = RecordingAsyncBroker {
            dequeued: vec![Ok(dequeued(message))],
            extend_lease_error: Some(LeaseError::Other("lease failed".to_owned())),
            ..RecordingAsyncBroker::default()
        };
        let mut processor = AsyncProcessor::with_parts_hooks_and_lease_extender(
            broker,
            PendingAsyncHandler {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            DefaultRetryDelay,
            TestClock(now),
            DefaultIsFailure,
            NoopErrorHandler,
            AsyncExtendLeaseWhileProcessing::every(Duration::from_millis(5)),
        );

        let error = processor
            .run_once(&["critical".to_owned()])
            .await
            .unwrap_err();

        assert_eq!(
            error,
            ProcessorError::Lease(LeaseError::Other("lease failed".to_owned()))
        );
        assert!(processor.broker().completed.is_empty());
        assert!(processor.broker().retried.is_empty());
        assert!(processor.broker().archived.is_empty());
        assert_eq!(
            processor.broker().lease_extensions,
            [("critical".to_owned(), "task-id".to_owned())]
        );
    }

    #[tokio::test]
    async fn shutdown_requeues_active_task() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let broker = RecordingAsyncBroker::default();
        let mut processor = AsyncProcessor::with_parts(
            broker,
            |_task: &Task| Ok::<(), HandlerError>(()),
            DefaultRetryDelay,
            TestClock(now),
        );
        processor.active_message = Some(message("task-id"));

        processor.shutdown().await.unwrap();

        assert_eq!(processor.broker().requeued, ["task-id"]);
        assert!(processor.active_message.is_none());
    }

    #[tokio::test]
    async fn reports_idle_without_error() {
        let broker = RecordingAsyncBroker {
            dequeued: vec![Err(DequeueError::NoProcessableTask)],
            ..RecordingAsyncBroker::default()
        };
        let mut processor = AsyncProcessor::with_parts(
            broker,
            |_task: &Task| Ok::<(), HandlerError>(()),
            DefaultRetryDelay,
            SystemClock,
        );

        let result = processor.run_once(&["critical".to_owned()]).await.unwrap();

        assert_eq!(result, ProcessorRun::NoProcessableTask);
    }

    #[tokio::test]
    async fn handler_panic_is_retried_like_handler_failure() {
        let broker = RecordingAsyncBroker {
            dequeued: vec![Ok(dequeued(message("task-id")))],
            ..RecordingAsyncBroker::default()
        };
        let mut processor = AsyncProcessor::with_parts(
            broker,
            |_task: &Task| -> Result<(), HandlerError> { panic!("boom") },
            |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
            TestClock(UNIX_EPOCH),
        );

        let result = processor.run_once(&["critical".to_owned()]).await.unwrap();

        assert_eq!(
            result,
            ProcessorRun::Retried {
                task_id: "task-id".to_owned(),
                retry_at: UNIX_EPOCH + Duration::from_secs(60),
            }
        );
        assert!(processor.broker().completed.is_empty());
        assert_eq!(
            processor.broker().retried,
            [(
                "task-id".to_owned(),
                UNIX_EPOCH + Duration::from_secs(60),
                "boom".to_owned(),
                true
            )]
        );
        assert!(processor.broker().archived.is_empty());
    }

    #[tokio::test]
    async fn task_timeout_retries_when_handler_exceeds_timeout() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let mut message = message("task-id");
        message.timeout = 1;
        let broker = RecordingAsyncBroker {
            dequeued: vec![Ok(dequeued(message))],
            ..RecordingAsyncBroker::default()
        };
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let mut processor = AsyncProcessor::with_parts(
            broker,
            PendingAsyncHandler {
                calls: Arc::clone(&handler_calls),
            },
            |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
            TestClock(now),
        );

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            processor.run_once(&["critical".to_owned()]),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            result,
            ProcessorRun::Retried {
                task_id: "task-id".to_owned(),
                retry_at: now + Duration::from_secs(60),
            }
        );
        assert_eq!(
            processor.broker().retried,
            [(
                "task-id".to_owned(),
                now + Duration::from_secs(60),
                "context deadline exceeded".to_owned(),
                true
            )]
        );
        assert_eq!(handler_calls.load(Ordering::Relaxed), 1);
        assert!(processor.broker().completed.is_empty());
        assert!(processor.broker().archived.is_empty());
    }

    #[tokio::test]
    async fn expired_deadline_retries_without_calling_handler() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let mut message = message("task-id");
        message.deadline = 1_699_999_999;
        let broker = RecordingAsyncBroker {
            dequeued: vec![Ok(dequeued(message))],
            ..RecordingAsyncBroker::default()
        };
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let calls = Arc::clone(&handler_calls);
        let mut processor = AsyncProcessor::with_parts(
            broker,
            move |_task: &Task| {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok::<(), HandlerError>(())
            },
            |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
            TestClock(now),
        );

        let result = processor.run_once(&["critical".to_owned()]).await.unwrap();

        assert_eq!(
            result,
            ProcessorRun::Retried {
                task_id: "task-id".to_owned(),
                retry_at: now + Duration::from_secs(60),
            }
        );
        assert_eq!(handler_calls.load(Ordering::Relaxed), 0);
        assert_eq!(processor.broker().retried[0].2, "context deadline exceeded");
    }

    #[tokio::test]
    async fn timeout_and_deadline_use_earliest_deadline() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let mut message = message("task-id");
        message.timeout = 60;
        message.deadline = 1_700_000_001;
        let broker = RecordingAsyncBroker {
            dequeued: vec![Ok(dequeued(message))],
            ..RecordingAsyncBroker::default()
        };
        let mut processor = AsyncProcessor::with_parts(
            broker,
            PendingAsyncHandler {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
            TestClock(now),
        );

        let queues = ["critical".to_owned()];
        let result = {
            let result = processor.run_once(&queues);
            tokio::pin!(result);
            tokio::task::yield_now().await;
            result.await.unwrap()
        };

        assert_eq!(
            result,
            ProcessorRun::Retried {
                task_id: "task-id".to_owned(),
                retry_at: now + Duration::from_secs(60),
            }
        );
        assert_eq!(processor.broker().retried[0].2, "context deadline exceeded");
    }

    struct PendingAsyncHandler {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl AsyncHandler for PendingAsyncHandler {
        async fn process_task(&mut self, _task: &Task) -> Result<(), HandlerError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            std::future::pending::<()>().await;
            Ok(())
        }
    }

    struct SleepingAsyncHandler {
        duration: Duration,
    }

    #[async_trait]
    impl AsyncHandler for SleepingAsyncHandler {
        async fn process_task(&mut self, _task: &Task) -> Result<(), HandlerError> {
            tokio::time::sleep(self.duration).await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn runs_maintenance() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let broker = RecordingAsyncBroker {
            forward_scheduled: 2,
            forward_retry: 1,
            recover_result: RecoverResult::new(3, 4),
            ..RecordingAsyncBroker::default()
        };
        let mut processor = AsyncProcessor::with_parts(
            broker,
            |_task: &Task| Ok::<(), HandlerError>(()),
            DefaultRetryDelay,
            TestClock(now),
        );

        let result = processor
            .run_maintenance(&["critical".to_owned()])
            .await
            .unwrap();

        assert_eq!(result.forwarded_scheduled(), 2);
        assert_eq!(result.forwarded_retry(), 1);
        assert_eq!(result.recovered_retried(), 3);
        assert_eq!(result.recovered_archived(), 4);
    }
}
