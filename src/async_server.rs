use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::watch;

use crate::{ProcessorError, ProcessorRun};

pub const DEFAULT_ASYNC_SERVER_IDLE_SLEEP: Duration = Duration::from_secs(1);
pub const DEFAULT_ASYNC_SERVER_RECOVER_RETRY_DELAY: Duration = Duration::from_secs(60);

/// Minimal Tokio-native worker server loop.
///
/// Reference: Asynq v0.26.0 `Server.Run` / `Server.Start` coordinate worker
/// goroutines and background lifecycle components:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L663-L721>.
///
/// TODO: Move Redis broker execution, handler execution, lease extension,
/// shutdown requeue, and recover/forward intervals onto this async runtime
/// model.
#[derive(Debug, Clone)]
pub struct AsyncServer<P, S = TokioSleeper> {
    processor: P,
    queues: Arc<[String]>,
    idle_sleep: Duration,
    sleeper: S,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct TokioSleeper;

#[async_trait]
pub trait AsyncSleeper {
    async fn sleep(&mut self, duration: Duration);
}

#[async_trait]
pub trait AsyncWorkerProcessor {
    async fn run_once(&mut self, queues: &[String]) -> Result<ProcessorRun, ProcessorError>;

    async fn run_maintenance(
        &mut self,
        _queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessorError> {
        Ok(ServerMaintenanceRun::default())
    }

    async fn shutdown(&mut self) -> Result<(), ProcessorError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServerRunSummary {
    processed: usize,
    completed: usize,
    retried: usize,
    archived: usize,
    revoked: usize,
    idle_polls: usize,
    forwarded_scheduled: usize,
    forwarded_retry: usize,
    recovered_retried: usize,
    recovered_archived: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ServerMaintenanceRun {
    forwarded_scheduled: usize,
    forwarded_retry: usize,
    recovered_retried: usize,
    recovered_archived: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ServerError {
    #[error("server requires at least one queue")]
    EmptyQueueList,
    #[error("queue name must contain one or more characters")]
    EmptyQueueName,
    #[error("server requires at least one worker")]
    EmptyWorkerCount,
    #[error("server worker task panicked")]
    WorkerThreadPanicked,
    #[error("processor failed: {0}")]
    Processor(ProcessorError),
}

impl<P> AsyncServer<P, TokioSleeper> {
    pub fn new<I, Q>(processor: P, queues: I) -> Result<Self, ServerError>
    where
        I: IntoIterator<Item = Q>,
        Q: Into<String>,
    {
        Self::with_sleeper(processor, queues, TokioSleeper)
    }
}

impl<P, S> AsyncServer<P, S> {
    pub fn with_sleeper<I, Q>(processor: P, queues: I, sleeper: S) -> Result<Self, ServerError>
    where
        I: IntoIterator<Item = Q>,
        Q: Into<String>,
    {
        let queues = normalize_queues(queues)?;
        Ok(Self {
            processor,
            queues: Arc::from(queues),
            idle_sleep: DEFAULT_ASYNC_SERVER_IDLE_SLEEP,
            sleeper,
        })
    }

    pub fn with_idle_sleep(mut self, duration: Duration) -> Self {
        self.idle_sleep = duration;
        self
    }

    pub fn processor(&self) -> &P {
        &self.processor
    }

    pub fn processor_mut(&mut self) -> &mut P {
        &mut self.processor
    }

    pub fn queues(&self) -> &[String] {
        &self.queues
    }

    pub fn idle_sleep(&self) -> Duration {
        self.idle_sleep
    }

    pub fn sleeper(&self) -> &S {
        &self.sleeper
    }

    pub fn sleeper_mut(&mut self) -> &mut S {
        &mut self.sleeper
    }

    pub fn into_processor(self) -> P {
        self.processor
    }
}

impl<P, S> AsyncServer<P, S>
where
    P: AsyncWorkerProcessor + Send,
    S: AsyncSleeper + Send,
{
    pub async fn run_until_stopped(
        &mut self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<ServerRunSummary, ServerError> {
        run_loop(
            &mut self.processor,
            &self.queues,
            &mut self.sleeper,
            self.idle_sleep,
            &mut shutdown,
        )
        .await
    }
}

impl ServerRunSummary {
    pub fn processed(&self) -> usize {
        self.processed
    }

    pub fn completed(&self) -> usize {
        self.completed
    }

    pub fn retried(&self) -> usize {
        self.retried
    }

    pub fn archived(&self) -> usize {
        self.archived
    }

    pub fn revoked(&self) -> usize {
        self.revoked
    }

    pub fn idle_polls(&self) -> usize {
        self.idle_polls
    }

    pub fn forwarded_scheduled(&self) -> usize {
        self.forwarded_scheduled
    }

    pub fn forwarded_retry(&self) -> usize {
        self.forwarded_retry
    }

    pub fn recovered_retried(&self) -> usize {
        self.recovered_retried
    }

    pub fn recovered_archived(&self) -> usize {
        self.recovered_archived
    }

    pub(crate) fn record(&mut self, result: ProcessorRun) {
        self.processed += 1;
        match result {
            ProcessorRun::Completed { .. } => self.completed += 1,
            ProcessorRun::Retried { .. } => self.retried += 1,
            ProcessorRun::Archived { .. } => self.archived += 1,
            ProcessorRun::Revoked { .. } => self.revoked += 1,
            ProcessorRun::NoProcessableTask => self.idle_polls += 1,
        }
    }

    pub(crate) fn record_idle_poll(&mut self) {
        self.idle_polls += 1;
    }

    pub(crate) fn record_maintenance(&mut self, result: ServerMaintenanceRun) {
        self.forwarded_scheduled += result.forwarded_scheduled;
        self.forwarded_retry += result.forwarded_retry;
        self.recovered_retried += result.recovered_retried;
        self.recovered_archived += result.recovered_archived;
    }

    pub(crate) fn merge(&mut self, other: ServerRunSummary) {
        self.processed += other.processed;
        self.completed += other.completed;
        self.retried += other.retried;
        self.archived += other.archived;
        self.revoked += other.revoked;
        self.idle_polls += other.idle_polls;
        self.forwarded_scheduled += other.forwarded_scheduled;
        self.forwarded_retry += other.forwarded_retry;
        self.recovered_retried += other.recovered_retried;
        self.recovered_archived += other.recovered_archived;
    }
}

impl ServerMaintenanceRun {
    pub fn new(
        forwarded_scheduled: usize,
        forwarded_retry: usize,
        recovered_retried: usize,
        recovered_archived: usize,
    ) -> Self {
        Self {
            forwarded_scheduled,
            forwarded_retry,
            recovered_retried,
            recovered_archived,
        }
    }

    pub fn forwarded_scheduled(&self) -> usize {
        self.forwarded_scheduled
    }

    pub fn forwarded_retry(&self) -> usize {
        self.forwarded_retry
    }

    pub fn recovered_retried(&self) -> usize {
        self.recovered_retried
    }

    pub fn recovered_archived(&self) -> usize {
        self.recovered_archived
    }

    pub fn total(&self) -> usize {
        self.forwarded_scheduled
            + self.forwarded_retry
            + self.recovered_retried
            + self.recovered_archived
    }
}

impl From<ProcessorError> for ServerError {
    fn from(error: ProcessorError) -> Self {
        Self::Processor(error)
    }
}

impl<P, S> AsyncServer<P, S>
where
    P: AsyncWorkerProcessor + Clone + Send + 'static,
    S: AsyncSleeper + Clone + Send + 'static,
{
    pub async fn run_until_stopped_parallel(
        self,
        worker_count: usize,
        shutdown: watch::Receiver<bool>,
    ) -> Result<ServerRunSummary, ServerError> {
        if worker_count == 0 {
            return Err(ServerError::EmptyWorkerCount);
        }

        let mut handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let mut server = self.clone();
            let shutdown = shutdown.clone();
            handles.push(tokio::spawn(async move {
                server.run_until_stopped(shutdown).await
            }));
        }

        let mut summary = ServerRunSummary::default();
        for handle in handles {
            let worker_summary = handle
                .await
                .map_err(|_| ServerError::WorkerThreadPanicked)??;
            summary.merge(worker_summary);
        }
        Ok(summary)
    }
}

#[async_trait]
impl AsyncSleeper for TokioSleeper {
    async fn sleep(&mut self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

async fn run_loop<P, S>(
    processor: &mut P,
    queues: &[String],
    sleeper: &mut S,
    idle_sleep: Duration,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<ServerRunSummary, ServerError>
where
    P: AsyncWorkerProcessor + Send,
    S: AsyncSleeper + Send,
{
    let mut summary = ServerRunSummary::default();
    while !*shutdown.borrow() {
        summary.record_maintenance(processor.run_maintenance(queues).await?);
        let run = tokio::select! {
            run = processor.run_once(queues) => run?,
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
                continue;
            }
        };
        match run {
            ProcessorRun::NoProcessableTask => {
                summary.record_idle_poll();
                tokio::select! {
                    _ = sleeper.sleep(idle_sleep) => {}
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                    }
                }
            }
            result => summary.record(result),
        }
    }
    processor.shutdown().await?;
    Ok(summary)
}

fn normalize_queues<I, Q>(queues: I) -> Result<Vec<String>, ServerError>
where
    I: IntoIterator<Item = Q>,
    Q: Into<String>,
{
    let queues: Vec<String> = queues.into_iter().map(Into::into).collect();
    if queues.is_empty() {
        return Err(ServerError::EmptyQueueList);
    }
    if queues.iter().any(|queue| queue.trim().is_empty()) {
        return Err(ServerError::EmptyQueueName);
    }
    Ok(queues)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Debug, Clone)]
    struct RecordingAsyncProcessor {
        results: Arc<Mutex<Vec<Result<ProcessorRun, ProcessorError>>>>,
        queue_calls: Arc<Mutex<Vec<Vec<String>>>>,
        maintenance_calls: Arc<Mutex<Vec<Vec<String>>>>,
        shutdown_calls: Arc<Mutex<usize>>,
    }

    #[async_trait]
    impl AsyncWorkerProcessor for RecordingAsyncProcessor {
        async fn run_once(&mut self, queues: &[String]) -> Result<ProcessorRun, ProcessorError> {
            self.queue_calls.lock().await.push(queues.to_vec());
            let mut results = self.results.lock().await;
            if results.is_empty() {
                Ok(ProcessorRun::NoProcessableTask)
            } else {
                results.remove(0)
            }
        }

        async fn run_maintenance(
            &mut self,
            queues: &[String],
        ) -> Result<ServerMaintenanceRun, ProcessorError> {
            self.maintenance_calls.lock().await.push(queues.to_vec());
            Ok(ServerMaintenanceRun::default())
        }

        async fn shutdown(&mut self) -> Result<(), ProcessorError> {
            *self.shutdown_calls.lock().await += 1;
            Ok(())
        }
    }

    #[derive(Debug, Clone, Default)]
    struct RecordingAsyncSleeper {
        durations: Arc<Mutex<Vec<Duration>>>,
    }

    #[async_trait]
    impl AsyncSleeper for RecordingAsyncSleeper {
        async fn sleep(&mut self, duration: Duration) {
            self.durations.lock().await.push(duration);
        }
    }

    #[tokio::test]
    async fn runs_until_shutdown_and_records_summary() {
        let processor = RecordingAsyncProcessor {
            results: Arc::new(Mutex::new(vec![
                Ok(ProcessorRun::Completed {
                    task_id: "task-id".to_owned(),
                }),
                Ok(ProcessorRun::NoProcessableTask),
            ])),
            queue_calls: Arc::new(Mutex::new(Vec::new())),
            maintenance_calls: Arc::new(Mutex::new(Vec::new())),
            shutdown_calls: Arc::new(Mutex::new(0)),
        };
        let sleeper = RecordingAsyncSleeper::default();
        let durations = Arc::clone(&sleeper.durations);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let server = AsyncServer::with_sleeper(processor, ["critical"], sleeper)
            .unwrap()
            .with_idle_sleep(Duration::from_millis(5));

        let summary = tokio::time::timeout(Duration::from_millis(100), async {
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let mut server = server;
            let handle = tokio::spawn(async move { server.run_until_stopped(shutdown_rx).await });
            tokio::task::yield_now().await;
            shutdown_tx.send(true).unwrap();
            handle.await.unwrap()
        })
        .await
        .unwrap()
        .unwrap();

        assert_eq!(summary.processed(), 1);
        assert_eq!(summary.completed(), 1);
        assert!(summary.idle_polls() >= 1);
        assert!(
            durations
                .lock()
                .await
                .iter()
                .all(|duration| *duration == Duration::from_millis(5))
        );
        drop(shutdown_rx);
    }

    #[derive(Debug, Clone, Default)]
    struct BlockingAsyncProcessor {
        shutdown_calls: Arc<Mutex<usize>>,
    }

    #[async_trait]
    impl AsyncWorkerProcessor for BlockingAsyncProcessor {
        async fn run_once(&mut self, _queues: &[String]) -> Result<ProcessorRun, ProcessorError> {
            std::future::pending().await
        }

        async fn shutdown(&mut self) -> Result<(), ProcessorError> {
            *self.shutdown_calls.lock().await += 1;
            Ok(())
        }
    }

    #[tokio::test]
    async fn shutdown_cancels_in_flight_run_and_calls_processor_shutdown() {
        let processor = BlockingAsyncProcessor::default();
        let shutdown_calls = Arc::clone(&processor.shutdown_calls);
        let mut server =
            AsyncServer::with_sleeper(processor, ["critical"], RecordingAsyncSleeper::default())
                .unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move { server.run_until_stopped(shutdown_rx).await });

        tokio::task::yield_now().await;
        shutdown_tx.send(true).unwrap();
        let summary = tokio::time::timeout(Duration::from_millis(100), handle)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(summary.processed(), 0);
        assert_eq!(*shutdown_calls.lock().await, 1);
    }

    #[tokio::test]
    async fn rejects_zero_parallel_workers() {
        let processor = RecordingAsyncProcessor {
            results: Arc::new(Mutex::new(Vec::new())),
            queue_calls: Arc::new(Mutex::new(Vec::new())),
            maintenance_calls: Arc::new(Mutex::new(Vec::new())),
            shutdown_calls: Arc::new(Mutex::new(0)),
        };
        let server = AsyncServer::new(processor, ["critical"]).unwrap();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let error = server
            .run_until_stopped_parallel(0, shutdown_rx)
            .await
            .unwrap_err();

        assert_eq!(error, ServerError::EmptyWorkerCount);
    }
}
