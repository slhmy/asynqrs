use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use crate::{
    ArchiveBroker, CompleteBroker, DequeueBroker, ErrorHandler, ForwardBroker, Handler, IsFailure,
    LeaseExtender, Processor, ProcessorError, ProcessorRun, RecoverBroker, RetryBroker, RetryDelay,
};

pub const DEFAULT_SERVER_IDLE_SLEEP: Duration = Duration::from_secs(1);
pub const DEFAULT_SERVER_RECOVER_RETRY_DELAY: Duration = Duration::from_secs(60);

/// Minimal synchronous worker server loop.
///
/// Reference: Asynq v0.26.0 `Server.Run` / `Server.Start` drive a processor
/// loop over configured queues:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L663-L721>.
///
/// TODO: Add worker concurrency, task context timeout/deadline handling, lease
/// extension, shutdown requeue, sync retry, and upstream maintenance intervals
/// once async/cancellation semantics are modeled.
#[derive(Debug, Clone)]
pub struct Server<P, S = SystemSleeper> {
    processor: P,
    queues: Vec<String>,
    idle_sleep: Duration,
    sleeper: S,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemSleeper;

pub trait Sleeper {
    fn sleep(&mut self, duration: Duration);
}

pub trait ShutdownSignal {
    fn should_stop(&mut self) -> bool;
}

pub trait WorkerProcessor {
    fn run_once(&mut self, queues: &[String]) -> Result<ProcessorRun, ProcessorError>;

    fn run_maintenance(
        &mut self,
        _queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessorError> {
        Ok(ServerMaintenanceRun::default())
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerError {
    EmptyQueueList,
    EmptyQueueName,
    EmptyWorkerCount,
    WorkerThreadPanicked,
    Processor(ProcessorError),
}

impl<P> Server<P, SystemSleeper> {
    pub fn new<I, Q>(processor: P, queues: I) -> Result<Self, ServerError>
    where
        I: IntoIterator<Item = Q>,
        Q: Into<String>,
    {
        Self::with_sleeper(processor, queues, SystemSleeper)
    }
}

impl<P, S> Server<P, S> {
    pub fn with_sleeper<I, Q>(processor: P, queues: I, sleeper: S) -> Result<Self, ServerError>
    where
        I: IntoIterator<Item = Q>,
        Q: Into<String>,
    {
        let queues = normalize_queues(queues)?;
        Ok(Self {
            processor,
            queues,
            idle_sleep: DEFAULT_SERVER_IDLE_SLEEP,
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

impl<P, S> Server<P, S> {
    fn run_loop<T>(
        processor: &mut P,
        queues: &[String],
        sleeper: &mut S,
        idle_sleep: Duration,
        shutdown: &mut T,
    ) -> Result<ServerRunSummary, ServerError>
    where
        P: WorkerProcessor,
        S: Sleeper,
        T: ShutdownSignal,
    {
        let mut summary = ServerRunSummary::default();
        while !shutdown.should_stop() {
            summary.record_maintenance(processor.run_maintenance(queues)?);
            match processor.run_once(queues)? {
                ProcessorRun::NoProcessableTask => {
                    summary.idle_polls += 1;
                    sleeper.sleep(idle_sleep);
                }
                result => summary.record(result),
            }
        }
        Ok(summary)
    }
}

impl<P, S> Server<P, S>
where
    P: WorkerProcessor,
    S: Sleeper,
{
    pub fn run_until_stopped<T>(
        &mut self,
        shutdown: &mut T,
    ) -> Result<ServerRunSummary, ServerError>
    where
        T: ShutdownSignal,
    {
        Self::run_loop(
            &mut self.processor,
            &self.queues,
            &mut self.sleeper,
            self.idle_sleep,
            shutdown,
        )
    }
}

impl<P, S> Server<P, S>
where
    P: WorkerProcessor + Clone + Send + 'static,
    S: Sleeper + Clone + Send + 'static,
{
    pub fn run_until_stopped_parallel(
        self,
        worker_count: usize,
        shutdown: Arc<AtomicBool>,
    ) -> Result<ServerRunSummary, ServerError> {
        if worker_count == 0 {
            return Err(ServerError::EmptyWorkerCount);
        }

        let mut handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let mut server = self.clone();
            let shutdown = Arc::clone(&shutdown);
            handles.push(std::thread::spawn(move || {
                let mut summary = ServerRunSummary::default();
                while !shutdown.load(Ordering::Relaxed) {
                    summary.record_maintenance(server.processor.run_maintenance(&server.queues)?);
                    match server.processor.run_once(&server.queues)? {
                        ProcessorRun::NoProcessableTask => {
                            summary.idle_polls += 1;
                            server.sleeper.sleep(server.idle_sleep);
                        }
                        result => summary.record(result),
                    }
                }
                Ok::<ServerRunSummary, ServerError>(summary)
            }));
        }

        let mut summary = ServerRunSummary::default();
        for handle in handles {
            let worker_summary = handle
                .join()
                .map_err(|_| ServerError::WorkerThreadPanicked)??;
            summary.merge(worker_summary);
        }
        Ok(summary)
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

impl Sleeper for SystemSleeper {
    fn sleep(&mut self, duration: Duration) {
        std::thread::sleep(duration);
    }
}

impl<F> ShutdownSignal for F
where
    F: FnMut() -> bool,
{
    fn should_stop(&mut self) -> bool {
        self()
    }
}

impl<B, H, R, C, I, E, L> WorkerProcessor for Processor<B, H, R, C, I, E, L>
where
    B: DequeueBroker + CompleteBroker + RetryBroker + ArchiveBroker + ForwardBroker + RecoverBroker,
    H: Handler,
    R: RetryDelay,
    C: crate::Clock,
    I: IsFailure,
    E: ErrorHandler,
    L: LeaseExtender<B>,
{
    fn run_once(&mut self, queues: &[String]) -> Result<ProcessorRun, ProcessorError> {
        Processor::run_once(self, queues)
    }

    fn run_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessorError> {
        Processor::run_maintenance(self, queues)
    }
}

impl From<ProcessorError> for ServerError {
    fn from(error: ProcessorError) -> Self {
        Self::Processor(error)
    }
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyQueueList => f.write_str("server requires at least one queue"),
            Self::EmptyQueueName => f.write_str("queue name must contain one or more characters"),
            Self::EmptyWorkerCount => f.write_str("server requires at least one worker"),
            Self::WorkerThreadPanicked => f.write_str("server worker thread panicked"),
            Self::Processor(error) => write!(f, "processor failed: {error}"),
        }
    }
}

impl std::error::Error for ServerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Processor(error) => Some(error),
            Self::EmptyQueueList
            | Self::EmptyQueueName
            | Self::EmptyWorkerCount
            | Self::WorkerThreadPanicked => None,
        }
    }
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
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use std::time::{Duration, UNIX_EPOCH};

    use crate::DequeueError;

    #[derive(Debug, Default)]
    struct RecordingProcessor {
        results: Vec<Result<ProcessorRun, ProcessorError>>,
        maintenance: Vec<Result<ServerMaintenanceRun, ProcessorError>>,
        queue_calls: Vec<Vec<String>>,
        maintenance_queue_calls: Vec<Vec<String>>,
    }

    impl WorkerProcessor for RecordingProcessor {
        fn run_once(&mut self, queues: &[String]) -> Result<ProcessorRun, ProcessorError> {
            self.queue_calls.push(queues.to_vec());
            self.results.remove(0)
        }

        fn run_maintenance(
            &mut self,
            queues: &[String],
        ) -> Result<ServerMaintenanceRun, ProcessorError> {
            self.maintenance_queue_calls.push(queues.to_vec());
            if self.maintenance.is_empty() {
                Ok(ServerMaintenanceRun::default())
            } else {
                self.maintenance.remove(0)
            }
        }
    }

    #[derive(Debug, Default)]
    struct RecordingSleeper {
        durations: Vec<Duration>,
    }

    impl Sleeper for RecordingSleeper {
        fn sleep(&mut self, duration: Duration) {
            self.durations.push(duration);
        }
    }

    #[derive(Debug)]
    struct StopAfter {
        remaining_runs: usize,
    }

    impl ShutdownSignal for StopAfter {
        fn should_stop(&mut self) -> bool {
            if self.remaining_runs == 0 {
                true
            } else {
                self.remaining_runs -= 1;
                false
            }
        }
    }

    #[test]
    fn validates_queues() {
        let empty = Server::with_sleeper(
            RecordingProcessor::default(),
            std::iter::empty::<String>(),
            RecordingSleeper::default(),
        )
        .unwrap_err();
        assert_eq!(empty, ServerError::EmptyQueueList);

        let blank = Server::with_sleeper(
            RecordingProcessor::default(),
            ["critical", " "],
            RecordingSleeper::default(),
        )
        .unwrap_err();
        assert_eq!(blank, ServerError::EmptyQueueName);
    }

    #[test]
    fn runs_processor_until_stopped_and_sleeps_when_idle() {
        let processor = RecordingProcessor {
            results: vec![
                Ok(ProcessorRun::Completed {
                    task_id: "completed-id".to_owned(),
                }),
                Ok(ProcessorRun::NoProcessableTask),
                Ok(ProcessorRun::Retried {
                    task_id: "retry-id".to_owned(),
                    retry_at: UNIX_EPOCH + Duration::from_secs(60),
                }),
                Ok(ProcessorRun::Archived {
                    task_id: "archive-id".to_owned(),
                }),
                Ok(ProcessorRun::Revoked {
                    task_id: "revoked-id".to_owned(),
                }),
            ],
            maintenance: vec![
                Ok(ServerMaintenanceRun::new(1, 0, 0, 0)),
                Ok(ServerMaintenanceRun::default()),
                Ok(ServerMaintenanceRun::new(0, 2, 1, 0)),
                Ok(ServerMaintenanceRun::default()),
                Ok(ServerMaintenanceRun::new(0, 0, 0, 1)),
            ],
            queue_calls: Vec::new(),
            maintenance_queue_calls: Vec::new(),
        };
        let mut server = Server::with_sleeper(
            processor,
            ["critical", "default"],
            RecordingSleeper::default(),
        )
        .unwrap()
        .with_idle_sleep(Duration::from_millis(25));
        let mut shutdown = StopAfter { remaining_runs: 5 };

        let summary = server.run_until_stopped(&mut shutdown).unwrap();

        assert_eq!(summary.processed(), 4);
        assert_eq!(summary.completed(), 1);
        assert_eq!(summary.retried(), 1);
        assert_eq!(summary.archived(), 1);
        assert_eq!(summary.revoked(), 1);
        assert_eq!(summary.idle_polls(), 1);
        assert_eq!(summary.forwarded_scheduled(), 1);
        assert_eq!(summary.forwarded_retry(), 2);
        assert_eq!(summary.recovered_retried(), 1);
        assert_eq!(summary.recovered_archived(), 1);
        assert_eq!(server.sleeper().durations, [Duration::from_millis(25)]);
        assert_eq!(
            server.processor().queue_calls,
            vec![
                vec!["critical".to_owned(), "default".to_owned()],
                vec!["critical".to_owned(), "default".to_owned()],
                vec!["critical".to_owned(), "default".to_owned()],
                vec!["critical".to_owned(), "default".to_owned()],
                vec!["critical".to_owned(), "default".to_owned()],
            ]
        );
        assert_eq!(
            server.processor().maintenance_queue_calls,
            vec![
                vec!["critical".to_owned(), "default".to_owned()],
                vec!["critical".to_owned(), "default".to_owned()],
                vec!["critical".to_owned(), "default".to_owned()],
                vec!["critical".to_owned(), "default".to_owned()],
                vec!["critical".to_owned(), "default".to_owned()],
            ]
        );
    }

    #[test]
    fn stops_before_polling_when_signal_is_already_set() {
        let processor = RecordingProcessor {
            results: vec![Ok(ProcessorRun::Completed {
                task_id: "task-id".to_owned(),
            })],
            maintenance: Vec::new(),
            queue_calls: Vec::new(),
            maintenance_queue_calls: Vec::new(),
        };
        let mut server =
            Server::with_sleeper(processor, ["critical"], RecordingSleeper::default()).unwrap();
        let mut shutdown = StopAfter { remaining_runs: 0 };

        let summary = server.run_until_stopped(&mut shutdown).unwrap();

        assert_eq!(summary, ServerRunSummary::default());
        assert!(server.processor().queue_calls.is_empty());
        assert!(server.processor().maintenance_queue_calls.is_empty());
    }

    #[test]
    fn propagates_processor_errors() {
        let processor = RecordingProcessor {
            results: vec![Err(ProcessorError::Dequeue(DequeueError::Other(
                "connection closed".to_owned(),
            )))],
            maintenance: Vec::new(),
            queue_calls: Vec::new(),
            maintenance_queue_calls: Vec::new(),
        };
        let mut server =
            Server::with_sleeper(processor, ["critical"], RecordingSleeper::default()).unwrap();
        let mut shutdown = StopAfter { remaining_runs: 1 };

        let error = server.run_until_stopped(&mut shutdown).unwrap_err();

        assert_eq!(
            error,
            ServerError::Processor(ProcessorError::Dequeue(DequeueError::Other(
                "connection closed".to_owned()
            )))
        );
    }

    #[test]
    fn propagates_maintenance_errors() {
        let processor = RecordingProcessor {
            results: vec![Ok(ProcessorRun::Completed {
                task_id: "task-id".to_owned(),
            })],
            maintenance: vec![Err(ProcessorError::Forward(crate::ForwardError::Other(
                "redis unavailable".to_owned(),
            )))],
            queue_calls: Vec::new(),
            maintenance_queue_calls: Vec::new(),
        };
        let mut server =
            Server::with_sleeper(processor, ["critical"], RecordingSleeper::default()).unwrap();
        let mut shutdown = StopAfter { remaining_runs: 1 };

        let error = server.run_until_stopped(&mut shutdown).unwrap_err();

        assert_eq!(
            error,
            ServerError::Processor(ProcessorError::Forward(crate::ForwardError::Other(
                "redis unavailable".to_owned()
            )))
        );
        assert!(server.processor().queue_calls.is_empty());
        assert_eq!(
            server.processor().maintenance_queue_calls,
            [vec!["critical".to_owned()]]
        );
    }

    #[test]
    fn runs_multiple_workers_in_parallel_and_merges_summary() {
        #[derive(Debug, Clone)]
        struct ParallelProcessor {
            state: Arc<std::sync::Mutex<Vec<Result<ProcessorRun, ProcessorError>>>>,
            queue_calls: Arc<std::sync::Mutex<usize>>,
            maintenance_calls: Arc<std::sync::Mutex<usize>>,
            shutdown: Arc<AtomicBool>,
        }

        impl WorkerProcessor for ParallelProcessor {
            fn run_once(&mut self, queues: &[String]) -> Result<ProcessorRun, ProcessorError> {
                *self.queue_calls.lock().unwrap() += 1;
                assert_eq!(queues, ["critical".to_owned()]);
                let mut state = self.state.lock().unwrap();
                if state.is_empty() {
                    self.shutdown.store(true, Ordering::Relaxed);
                    Ok(ProcessorRun::NoProcessableTask)
                } else {
                    let result = state.remove(0);
                    if state.is_empty() {
                        self.shutdown.store(true, Ordering::Relaxed);
                    }
                    result
                }
            }

            fn run_maintenance(
                &mut self,
                queues: &[String],
            ) -> Result<ServerMaintenanceRun, ProcessorError> {
                *self.maintenance_calls.lock().unwrap() += 1;
                assert_eq!(queues, ["critical".to_owned()]);
                Ok(ServerMaintenanceRun::default())
            }
        }

        #[derive(Debug, Clone, Default)]
        struct NoopSleeper;

        impl Sleeper for NoopSleeper {
            fn sleep(&mut self, _duration: Duration) {}
        }

        let shutdown = Arc::new(AtomicBool::new(false));
        let processor = ParallelProcessor {
            state: Arc::new(std::sync::Mutex::new(vec![
                Ok(ProcessorRun::Completed {
                    task_id: "task-a".to_owned(),
                }),
                Ok(ProcessorRun::Completed {
                    task_id: "task-b".to_owned(),
                }),
            ])),
            queue_calls: Arc::new(std::sync::Mutex::new(0)),
            maintenance_calls: Arc::new(std::sync::Mutex::new(0)),
            shutdown: Arc::clone(&shutdown),
        };
        let server = Server::with_sleeper(processor, ["critical"], NoopSleeper).unwrap();

        let summary = server
            .run_until_stopped_parallel(2, Arc::clone(&shutdown))
            .unwrap();

        assert_eq!(summary.processed(), 2);
        assert_eq!(summary.completed(), 2);
        assert_eq!(summary.retried(), 0);
        assert_eq!(summary.archived(), 0);
        assert_eq!(summary.revoked(), 0);
        assert!(summary.idle_polls() <= 2);
    }
}
