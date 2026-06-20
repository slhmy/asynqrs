use std::sync::Arc;
use std::time::SystemTime;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::operation::ResultWrite;
use crate::server::{LogLevel, Logger, WorkerBrokerCore};
use crate::task::TaskMessage;
use crate::{ProcessingContext, ResultError, Task};

use super::super::lease::lease_expired_error;
use super::super::{Handler, HandlerError, LeaseExtender, ProcessingError, ProcessingLease};
use super::handler::{HandlerExecution, perform};
use crate::server::tokio_instant_for_system_time;

pub(crate) struct TaskExecutionContext<'a> {
    pub(crate) message: &'a TaskMessage,
    pub(crate) task: &'a Task,
    pub(crate) context: &'a ProcessingContext,
    pub(crate) lease: &'a ProcessingLease,
    pub(crate) deadline: Option<SystemTime>,
    pub(crate) now: SystemTime,
    pub(crate) cancellation: CancellationToken,
    pub(crate) logger: &'a Option<Arc<dyn Logger>>,
    pub(crate) log_level: LogLevel,
    pub(crate) result_writes: mpsc::UnboundedReceiver<ResultWrite>,
}

pub(crate) async fn perform_with_lease_extender<H, L, B>(
    handler: &mut H,
    lease_extender: &mut L,
    broker: &mut B,
    context: TaskExecutionContext<'_>,
) -> Result<Result<(), HandlerError>, ProcessingError>
where
    H: Handler + Send,
    L: LeaseExtender<B> + Send,
    B: WorkerBrokerCore + Send,
{
    let Some(interval) = lease_extender.during_process_interval() else {
        return perform_with_result_writer(handler, broker, context).await;
    };

    if context
        .deadline
        .is_some_and(|deadline| deadline <= context.now)
    {
        return Ok(Err(HandlerError::failed("context deadline exceeded")));
    }

    let handler = perform(
        handler,
        HandlerExecution {
            task: context.task,
            context: context.context,
            deadline: context.deadline,
            now: context.now,
            cancellation: context.cancellation,
            logger: context.logger,
            log_level: context.log_level,
        },
    );
    tokio::pin!(handler);
    let mut lease_interval = tokio::time::interval(interval);
    lease_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut result_writes = context.result_writes;

    loop {
        tokio::select! {
            result = &mut handler => {
                flush_result_writes(broker, context.message, &mut result_writes).await?;
                return Ok(result);
            }
            expired = wait_until_lease_expires(context.lease, context.now) => {
                // Reference: Asynq v0.26.0 handles active lease expiration
                // while the handler is still running as `HandlerError::LeaseExpired`:
                // <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L241-L246>.
                if expired {
                    context.lease.expire_before(context.now);
                    return Ok(Err(lease_expired_error()));
                }
            }
            _ = lease_interval.tick() => {
                if let Some(extension) = lease_extender.during_process(broker, context.message).await? {
                    context.lease.reset(extension);
                }
            }
            Some(write) = result_writes.recv() => {
                write_result_and_ack(broker, context.message, write).await?;
            }
        }
    }
}

async fn perform_with_result_writer<H, B>(
    handler: &mut H,
    broker: &mut B,
    context: TaskExecutionContext<'_>,
) -> Result<Result<(), HandlerError>, ProcessingError>
where
    H: Handler + Send,
    B: WorkerBrokerCore + Send,
{
    let handler = perform(
        handler,
        HandlerExecution {
            task: context.task,
            context: context.context,
            deadline: context.deadline,
            now: context.now,
            cancellation: context.cancellation,
            logger: context.logger,
            log_level: context.log_level,
        },
    );
    tokio::pin!(handler);
    let mut result_writes = context.result_writes;

    loop {
        tokio::select! {
            result = &mut handler => {
                flush_result_writes(broker, context.message, &mut result_writes).await?;
                return Ok(result);
            }
            expired = wait_until_lease_expires(context.lease, context.now) => {
                // Reference: Asynq v0.26.0 handles active lease expiration
                // while the handler is still running as `HandlerError::LeaseExpired`:
                // <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L241-L246>.
                if expired {
                    context.lease.expire_before(context.now);
                    return Ok(Err(lease_expired_error()));
                }
            }
            Some(write) = result_writes.recv() => {
                write_result_and_ack(broker, context.message, write).await?;
            }
        }
    }
}

async fn wait_until_lease_expires(lease: &ProcessingLease, now: SystemTime) -> bool {
    let expires_at = lease.expires_at();
    if expires_at < now {
        true
    } else if let Some(deadline) = tokio_instant_for_system_time(expires_at, now) {
        tokio::time::sleep_until(deadline).await;
        lease.expires_at() <= expires_at
    } else {
        std::future::pending::<bool>().await
    }
}

async fn flush_result_writes<B>(
    broker: &mut B,
    message: &TaskMessage,
    result_writes: &mut mpsc::UnboundedReceiver<ResultWrite>,
) -> Result<(), ProcessingError>
where
    B: WorkerBrokerCore + Send,
{
    while let Ok(write) = result_writes.try_recv() {
        write_result_and_ack(broker, message, write).await?;
    }
    Ok(())
}

async fn write_result_and_ack<B>(
    broker: &mut B,
    message: &TaskMessage,
    write: ResultWrite,
) -> Result<(), ProcessingError>
where
    B: WorkerBrokerCore + Send,
{
    if let Some(error) = write.context_error() {
        if let Some(ack) = write.ack {
            let _ = ack.send(Err(error));
        }
        return Ok(());
    }
    let ResultWrite {
        data,
        ack,
        cancellation,
        parent_cancellation,
        deadline,
    } = write;
    let result = write_result_with_context(
        broker,
        &message.queue,
        &message.id,
        data,
        cancellation,
        parent_cancellation,
        deadline,
    )
    .await;
    if let Some(ack) = ack {
        let _ = ack.send(result.clone());
    }
    if result.as_ref().is_err_and(is_context_result_error) {
        return Ok(());
    }
    result?;
    Ok(())
}

async fn write_result_with_context<B>(
    broker: &mut B,
    queue: &str,
    task_id: &str,
    data: Bytes,
    cancellation: Option<CancellationToken>,
    parent_cancellation: Option<CancellationToken>,
    deadline: Option<Instant>,
) -> Result<usize, ResultError>
where
    B: WorkerBrokerCore + Send,
{
    let write = WorkerBrokerCore::write_result(broker, queue, task_id, data.to_vec());
    tokio::pin!(write);

    match (cancellation, parent_cancellation, deadline) {
        (None, None, None) => write.await,
        (cancellation, parent_cancellation, deadline) => {
            tokio::select! {
                result = &mut write => result,
                _ = wait_for_result_cancellation(cancellation, parent_cancellation) => {
                    Err(ResultError::WriteFailed("context canceled".to_owned()))
                }
                _ = wait_for_result_deadline(deadline) => {
                    Err(ResultError::WriteFailed("context deadline exceeded".to_owned()))
                }
            }
        }
    }
}

async fn wait_for_result_cancellation(
    cancellation: Option<CancellationToken>,
    parent_cancellation: Option<CancellationToken>,
) {
    match (cancellation, parent_cancellation) {
        (Some(cancellation), Some(parent)) => {
            tokio::select! {
                _ = cancellation.cancelled() => {}
                _ = parent.cancelled() => {}
            }
        }
        (Some(cancellation), None) => cancellation.cancelled().await,
        (None, Some(parent)) => parent.cancelled().await,
        (None, None) => std::future::pending::<()>().await,
    }
}

async fn wait_for_result_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending::<()>().await,
    }
}

fn is_context_result_error(error: &ResultError) -> bool {
    matches!(
        error,
        ResultError::WriteFailed(message)
            if message == "context canceled" || message == "context deadline exceeded"
    )
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use tokio::sync::{Notify, oneshot, watch};

    use super::*;

    #[derive(Default)]
    struct RecordingResultBroker {
        writes: Vec<(String, String, Vec<u8>)>,
    }

    struct BlockingResultBroker {
        started: Arc<Notify>,
        finished: Arc<AtomicUsize>,
        finish: watch::Receiver<bool>,
    }

    #[async_trait::async_trait]
    impl WorkerBrokerCore for RecordingResultBroker {
        async fn dequeue(
            &mut self,
            _queues: &[String],
        ) -> Result<crate::DequeuedTask, crate::DequeueError> {
            unreachable!("result writer tests do not dequeue")
        }

        async fn complete(&mut self, _message: &TaskMessage) -> Result<(), crate::CompleteError> {
            unreachable!("result writer tests do not complete tasks")
        }

        async fn retry(
            &mut self,
            _message: &TaskMessage,
            _retry_at: std::time::SystemTime,
            _error_message: &str,
            _is_failure: bool,
        ) -> Result<(), crate::RetryError> {
            unreachable!("result writer tests do not retry tasks")
        }

        async fn archive(
            &mut self,
            _message: &TaskMessage,
            _error_message: &str,
        ) -> Result<(), crate::ArchiveError> {
            unreachable!("result writer tests do not archive tasks")
        }

        async fn write_result(
            &mut self,
            queue: &str,
            task_id: &str,
            data: Vec<u8>,
        ) -> Result<usize, ResultError> {
            let len = data.len();
            self.writes
                .push((queue.to_owned(), task_id.to_owned(), data));
            Ok(len)
        }
    }

    #[async_trait::async_trait]
    impl WorkerBrokerCore for BlockingResultBroker {
        async fn dequeue(
            &mut self,
            _queues: &[String],
        ) -> Result<crate::DequeuedTask, crate::DequeueError> {
            unreachable!("result writer tests do not dequeue")
        }

        async fn complete(&mut self, _message: &TaskMessage) -> Result<(), crate::CompleteError> {
            unreachable!("result writer tests do not complete tasks")
        }

        async fn retry(
            &mut self,
            _message: &TaskMessage,
            _retry_at: std::time::SystemTime,
            _error_message: &str,
            _is_failure: bool,
        ) -> Result<(), crate::RetryError> {
            unreachable!("result writer tests do not retry tasks")
        }

        async fn archive(
            &mut self,
            _message: &TaskMessage,
            _error_message: &str,
        ) -> Result<(), crate::ArchiveError> {
            unreachable!("result writer tests do not archive tasks")
        }

        async fn write_result(
            &mut self,
            _queue: &str,
            _task_id: &str,
            data: Vec<u8>,
        ) -> Result<usize, ResultError> {
            self.started.notify_one();
            while !*self.finish.borrow() {
                if self.finish.changed().await.is_err() {
                    break;
                }
            }
            self.finished.fetch_add(1, Ordering::Relaxed);
            Ok(data.len())
        }
    }

    #[tokio::test]
    async fn queued_result_write_skips_broker_after_context_cancellation_like_upstream() {
        let cancellation = CancellationToken::new();
        let (ack, wait_for_ack) = oneshot::channel();
        let mut broker = RecordingResultBroker::default();
        let message = task_message();
        let write = ResultWrite {
            data: Bytes::from_static(b"handler-result"),
            ack: Some(ack),
            cancellation: Some(cancellation.clone()),
            parent_cancellation: None,
            deadline: None,
        };
        cancellation.cancel();

        write_result_and_ack(&mut broker, &message, write)
            .await
            .unwrap();

        assert!(broker.writes.is_empty());
        assert_eq!(
            wait_for_ack.await.unwrap().unwrap_err(),
            ResultError::WriteFailed("context canceled".to_owned())
        );
    }

    #[tokio::test]
    async fn queued_result_write_skips_broker_after_context_deadline_like_upstream() {
        let (ack, wait_for_ack) = oneshot::channel();
        let mut broker = RecordingResultBroker::default();
        let message = task_message();
        let write = ResultWrite {
            data: Bytes::from_static(b"handler-result"),
            ack: Some(ack),
            cancellation: None,
            parent_cancellation: None,
            deadline: tokio::time::Instant::now().checked_sub(Duration::from_millis(1)),
        };

        write_result_and_ack(&mut broker, &message, write)
            .await
            .unwrap();

        assert!(broker.writes.is_empty());
        assert_eq!(
            wait_for_ack.await.unwrap().unwrap_err(),
            ResultError::WriteFailed("context deadline exceeded".to_owned())
        );
    }

    #[tokio::test]
    async fn queued_result_write_cancels_in_flight_broker_write_like_upstream() {
        let cancellation = CancellationToken::new();
        let (_finish, finish) = watch::channel(false);
        let (ack, wait_for_ack) = oneshot::channel();
        let started = Arc::new(Notify::new());
        let finished = Arc::new(AtomicUsize::new(0));
        let mut broker = BlockingResultBroker {
            started: Arc::clone(&started),
            finished: Arc::clone(&finished),
            finish,
        };
        let message = task_message();
        let write = ResultWrite {
            data: Bytes::from_static(b"handler-result"),
            ack: Some(ack),
            cancellation: Some(cancellation.clone()),
            parent_cancellation: None,
            deadline: None,
        };
        let run = write_result_and_ack(&mut broker, &message, write);
        tokio::pin!(run);
        tokio::select! {
            _ = started.notified() => {}
            result = &mut run => panic!("broker write finished before cancellation: {result:?}"),
        }

        cancellation.cancel();
        run.await.unwrap();

        assert_eq!(finished.load(Ordering::Relaxed), 0);
        assert_eq!(
            wait_for_ack.await.unwrap().unwrap_err(),
            ResultError::WriteFailed("context canceled".to_owned())
        );
    }

    #[tokio::test]
    async fn queued_result_write_deadline_cancels_in_flight_broker_write_like_upstream() {
        let (_finish, finish) = watch::channel(false);
        let (ack, wait_for_ack) = oneshot::channel();
        let started = Arc::new(Notify::new());
        let finished = Arc::new(AtomicUsize::new(0));
        let mut broker = BlockingResultBroker {
            started: Arc::clone(&started),
            finished: Arc::clone(&finished),
            finish,
        };
        let message = task_message();
        let write = ResultWrite {
            data: Bytes::from_static(b"handler-result"),
            ack: Some(ack),
            cancellation: None,
            parent_cancellation: None,
            deadline: tokio::time::Instant::now().checked_add(Duration::from_millis(100)),
        };
        let run = write_result_and_ack(&mut broker, &message, write);
        tokio::pin!(run);
        tokio::select! {
            _ = started.notified() => {}
            result = &mut run => panic!("broker write finished before deadline: {result:?}"),
        }

        run.await.unwrap();

        assert_eq!(finished.load(Ordering::Relaxed), 0);
        assert_eq!(
            wait_for_ack.await.unwrap().unwrap_err(),
            ResultError::WriteFailed("context deadline exceeded".to_owned())
        );
    }

    fn task_message() -> TaskMessage {
        TaskMessage {
            id: "task-id".to_owned(),
            queue: "critical".to_owned(),
            ..TaskMessage::default()
        }
    }
}
