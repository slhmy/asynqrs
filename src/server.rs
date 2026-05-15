use std::time::Duration;

use crate::{
    ArchiveBroker, CompleteBroker, DequeueBroker, ErrorHandler, Handler, IsFailure, Processor,
    ProcessorError, ProcessorRun, RetryBroker, RetryDelay,
};

pub const DEFAULT_SERVER_IDLE_SLEEP: Duration = Duration::from_secs(1);

/// Minimal synchronous worker server loop.
///
/// Reference: Asynq v0.26.0 `Server.Run` / `Server.Start` drive a processor
/// loop over configured queues:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L663-L721>.
///
/// TODO: Add worker concurrency, task context timeout/deadline handling, lease
/// extension, shutdown requeue, and sync retry once async/cancellation
/// semantics are modeled.
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
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServerRunSummary {
    processed: usize,
    completed: usize,
    retried: usize,
    archived: usize,
    revoked: usize,
    idle_polls: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerError {
    EmptyQueueList,
    EmptyQueueName,
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
        let mut summary = ServerRunSummary::default();
        while !shutdown.should_stop() {
            match self.processor.run_once(&self.queues)? {
                ProcessorRun::NoProcessableTask => {
                    summary.idle_polls += 1;
                    self.sleeper.sleep(self.idle_sleep);
                }
                result => summary.record(result),
            }
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

    fn record(&mut self, result: ProcessorRun) {
        self.processed += 1;
        match result {
            ProcessorRun::Completed { .. } => self.completed += 1,
            ProcessorRun::Retried { .. } => self.retried += 1,
            ProcessorRun::Archived { .. } => self.archived += 1,
            ProcessorRun::Revoked { .. } => self.revoked += 1,
            ProcessorRun::NoProcessableTask => self.idle_polls += 1,
        }
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

impl<B, H, R, C, I, E> WorkerProcessor for Processor<B, H, R, C, I, E>
where
    B: DequeueBroker + CompleteBroker + RetryBroker + ArchiveBroker,
    H: Handler,
    R: RetryDelay,
    C: crate::Clock,
    I: IsFailure,
    E: ErrorHandler,
{
    fn run_once(&mut self, queues: &[String]) -> Result<ProcessorRun, ProcessorError> {
        Processor::run_once(self, queues)
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
            Self::Processor(error) => write!(f, "processor failed: {error}"),
        }
    }
}

impl std::error::Error for ServerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Processor(error) => Some(error),
            Self::EmptyQueueList | Self::EmptyQueueName => None,
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
    use std::time::{Duration, UNIX_EPOCH};

    use crate::DequeueError;

    #[derive(Debug, Default)]
    struct RecordingProcessor {
        results: Vec<Result<ProcessorRun, ProcessorError>>,
        queue_calls: Vec<Vec<String>>,
    }

    impl WorkerProcessor for RecordingProcessor {
        fn run_once(&mut self, queues: &[String]) -> Result<ProcessorRun, ProcessorError> {
            self.queue_calls.push(queues.to_vec());
            self.results.remove(0)
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
            queue_calls: Vec::new(),
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
    }

    #[test]
    fn stops_before_polling_when_signal_is_already_set() {
        let processor = RecordingProcessor {
            results: vec![Ok(ProcessorRun::Completed {
                task_id: "task-id".to_owned(),
            })],
            queue_calls: Vec::new(),
        };
        let mut server =
            Server::with_sleeper(processor, ["critical"], RecordingSleeper::default()).unwrap();
        let mut shutdown = StopAfter { remaining_runs: 0 };

        let summary = server.run_until_stopped(&mut shutdown).unwrap();

        assert_eq!(summary, ServerRunSummary::default());
        assert!(server.processor().queue_calls.is_empty());
    }

    #[test]
    fn propagates_processor_errors() {
        let processor = RecordingProcessor {
            results: vec![Err(ProcessorError::Dequeue(DequeueError::Other(
                "connection closed".to_owned(),
            )))],
            queue_calls: Vec::new(),
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
}
