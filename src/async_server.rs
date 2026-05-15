use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::watch;

use crate::{ProcessorError, ProcessorRun, ServerError, ServerMaintenanceRun, ServerRunSummary};

pub const DEFAULT_ASYNC_SERVER_IDLE_SLEEP: Duration = Duration::from_secs(1);

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
        match processor.run_once(queues).await? {
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

    #[tokio::test]
    async fn rejects_zero_parallel_workers() {
        let processor = RecordingAsyncProcessor {
            results: Arc::new(Mutex::new(Vec::new())),
            queue_calls: Arc::new(Mutex::new(Vec::new())),
            maintenance_calls: Arc::new(Mutex::new(Vec::new())),
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
