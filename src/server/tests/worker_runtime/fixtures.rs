use super::*;
use crate::BrokerError;
use crate::EnqueuePlan;
use crate::aggregation::{AggregationBroker, AggregationSet};
use crate::client::CloseBroker;
use crate::server::{
    ArchiveBroker, CleanupBroker, ForwardBroker, LeaseBroker, MetadataBroker, PingBroker,
    RecoverBroker, RequeueBroker, RetryBroker, WorkerBrokerCore,
};

#[derive(Debug, Default, Clone)]
pub(super) struct RecordingBroker {
    pub(super) dequeued: Vec<Result<DequeuedTask, DequeueError>>,
    pub(super) completed: Vec<String>,
    pub(super) complete_error: Option<CompleteError>,
    pub(super) retried: Vec<(String, SystemTime, String, bool)>,
    pub(super) retry_error: Option<RetryError>,
    pub(super) archived: Vec<(String, String)>,
    pub(super) archive_error: Option<ArchiveError>,
    pub(super) requeued: Vec<String>,
    pub(super) requeue_error: Option<RequeueError>,
    pub(super) results: Vec<(String, String, Vec<u8>)>,
    pub(super) lease_extensions: Vec<(String, String)>,
    pub(super) lease_extension_batches: Vec<(String, Vec<String>)>,
    pub(super) extend_lease_error: Option<LeaseError>,
    pub(super) forward_queue_calls: Vec<Vec<String>>,
    pub(super) forward_scheduled: usize,
    pub(super) forward_scheduled_error: Option<ForwardError>,
    pub(super) forward_retry: usize,
    pub(super) forward_retry_error: Option<ForwardError>,
    pub(super) lease_expired: Vec<TaskMessage>,
    pub(super) lease_expired_error: Option<RecoverError>,
    pub(super) lease_expired_cutoffs: Vec<SystemTime>,
    pub(super) maintenance_events: Vec<String>,
    pub(super) reclaimed_stale_aggregation_sets: Vec<(String, SystemTime)>,
    pub(super) reclaim_stale_aggregation_sets_error: Option<AggregationError>,
    pub(super) deleted_expired_completed: usize,
    pub(super) cleanup_batches: Vec<(String, isize)>,
    pub(super) delete_expired_completed_error: Option<CleanupError>,
    pub(super) delete_expired_completed_error_queues: Vec<String>,
}

#[async_trait]
impl WorkerBrokerCore for RecordingBroker {
    async fn dequeue(&mut self, _queues: &[String]) -> Result<DequeuedTask, DequeueError> {
        self.dequeued.remove(0)
    }

    async fn complete(&mut self, message: &TaskMessage) -> Result<(), CompleteError> {
        self.completed.push(message.id.clone());
        if let Some(error) = self.complete_error.clone() {
            return Err(error);
        }
        Ok(())
    }

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
        if let Some(error) = self.retry_error.clone() {
            return Err(error);
        }
        Ok(())
    }

    async fn archive(
        &mut self,
        message: &TaskMessage,
        error_message: &str,
    ) -> Result<(), ArchiveError> {
        self.archived
            .push((message.id.clone(), error_message.to_owned()));
        if let Some(error) = self.archive_error.clone() {
            return Err(error);
        }
        Ok(())
    }

    async fn write_result(
        &mut self,
        queue: &str,
        task_id: &str,
        data: Vec<u8>,
    ) -> Result<usize, ResultError> {
        let len = data.len();
        self.results
            .push((queue.to_owned(), task_id.to_owned(), data));
        Ok(len)
    }
}

#[async_trait]
impl RetryBroker for RecordingBroker {
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
        if let Some(error) = self.retry_error.clone() {
            return Err(error);
        }
        Ok(())
    }
}

#[async_trait]
impl ArchiveBroker for RecordingBroker {
    async fn archive(
        &mut self,
        message: &TaskMessage,
        error_message: &str,
    ) -> Result<(), ArchiveError> {
        self.archived
            .push((message.id.clone(), error_message.to_owned()));
        if let Some(error) = self.archive_error.clone() {
            return Err(error);
        }
        Ok(())
    }
}

#[async_trait]
impl LeaseBroker for RecordingBroker {
    async fn extend_leases(
        &mut self,
        queue: &str,
        task_ids: &[String],
    ) -> Result<LeaseExtension, LeaseError> {
        self.lease_extension_batches
            .push((queue.to_owned(), task_ids.to_vec()));
        self.lease_extensions.extend(
            task_ids
                .iter()
                .map(|task_id| (queue.to_owned(), task_id.clone())),
        );
        if let Some(error) = self.extend_lease_error.clone() {
            return Err(error);
        }
        Ok(LeaseExtension::new(
            UNIX_EPOCH + Duration::from_secs(1_700_000_030),
        ))
    }
}

#[async_trait]
impl RequeueBroker for RecordingBroker {
    async fn requeue(&mut self, message: &TaskMessage) -> Result<(), RequeueError> {
        self.requeued.push(message.id.clone());
        if let Some(error) = self.requeue_error.clone() {
            return Err(error);
        }
        Ok(())
    }
}

#[async_trait]
impl ForwardBroker for RecordingBroker {
    async fn forward_if_ready(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ForwardError> {
        self.forward_queue_calls.push(queues.to_vec());
        if let Some(error) = self.forward_scheduled_error.clone() {
            return Err(error);
        }
        if let Some(error) = self.forward_retry_error.clone() {
            return Err(error);
        }
        Ok(ServerMaintenanceRun::new(
            self.forward_scheduled,
            self.forward_retry,
            0,
            0,
            0,
        ))
    }
}

#[async_trait]
impl RecoverBroker for RecordingBroker {
    async fn list_lease_expired(
        &mut self,
        cutoff: SystemTime,
        queues: &[String],
    ) -> Result<Vec<TaskMessage>, RecoverError> {
        self.lease_expired_cutoffs.push(cutoff);
        self.maintenance_events
            .push(format!("recover:{}", queues.join(",")));
        if let Some(error) = self.lease_expired_error.clone() {
            return Err(error);
        }
        Ok(std::mem::take(&mut self.lease_expired))
    }
}

#[async_trait]
impl CleanupBroker for RecordingBroker {
    async fn delete_expired_completed_tasks(
        &mut self,
        queue: &str,
        batch_size: isize,
    ) -> Result<usize, CleanupError> {
        self.cleanup_batches.push((queue.to_owned(), batch_size));
        if let Some(error) = self.delete_expired_completed_error.clone() {
            return Err(error);
        }
        if self
            .delete_expired_completed_error_queues
            .iter()
            .any(|error_queue| error_queue == queue)
        {
            return Err(CleanupError::Other("cleanup down".to_owned()));
        }
        Ok(self.deleted_expired_completed)
    }
}

#[async_trait]
impl MetadataBroker for RecordingBroker {
    async fn write_server_state(
        &mut self,
        _hostname: &str,
        _pid: i32,
        _server_id: &str,
        _server_info: Vec<u8>,
        _workers: Vec<Vec<u8>>,
        _ttl: Duration,
    ) -> Result<(), MetadataError> {
        Ok(())
    }

    async fn clear_server_state(
        &mut self,
        _hostname: &str,
        _pid: i32,
        _server_id: &str,
    ) -> Result<(), MetadataError> {
        Ok(())
    }
}

#[async_trait]
impl PingBroker for RecordingBroker {
    async fn ping(&mut self) -> Result<(), String> {
        Ok(())
    }
}

impl CloseBroker for RecordingBroker {
    fn close(&mut self) -> Result<(), BrokerError> {
        Ok(())
    }
}

#[async_trait]
impl AggregationBroker for RecordingBroker {
    async fn list_aggregation_groups(
        &mut self,
        _queue: &str,
    ) -> Result<Vec<String>, AggregationError> {
        unreachable!("worker runtime maintenance tests only reclaim stale aggregation sets")
    }

    async fn aggregation_check(
        &mut self,
        _queue: &str,
        _group: &str,
        _now: SystemTime,
        _grace_period: Duration,
        _max_delay: Duration,
        _max_size: isize,
    ) -> Result<Option<String>, AggregationError> {
        unreachable!("worker runtime maintenance tests only reclaim stale aggregation sets")
    }

    async fn read_aggregation_set(
        &mut self,
        _queue: &str,
        _group: &str,
        _set_id: &str,
    ) -> Result<AggregationSet, AggregationError> {
        unreachable!("worker runtime maintenance tests only reclaim stale aggregation sets")
    }

    async fn delete_aggregation_set(
        &mut self,
        _queue: &str,
        _group: &str,
        _set_id: &str,
    ) -> Result<(), AggregationError> {
        unreachable!("worker runtime maintenance tests only reclaim stale aggregation sets")
    }

    async fn enqueue_aggregated(&mut self, _plan: &EnqueuePlan) -> Result<(), BrokerError> {
        unreachable!("worker runtime maintenance tests only reclaim stale aggregation sets")
    }

    async fn reclaim_stale_aggregation_sets(
        &mut self,
        queue: &str,
        now: SystemTime,
    ) -> Result<(), AggregationError> {
        self.maintenance_events.push(format!("reclaim:{queue}"));
        self.reclaimed_stale_aggregation_sets
            .push((queue.to_owned(), now));
        if let Some(error) = self.reclaim_stale_aggregation_sets_error.clone() {
            return Err(error);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct TestClock(pub(super) SystemTime);

impl Clock for TestClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

#[derive(Debug, Clone)]
pub(super) struct SequenceClock {
    times: Arc<Mutex<Vec<SystemTime>>>,
}

impl SequenceClock {
    pub(super) fn new(times: impl IntoIterator<Item = SystemTime>) -> Self {
        Self {
            times: Arc::new(Mutex::new(times.into_iter().collect())),
        }
    }
}

impl Clock for SequenceClock {
    fn now(&self) -> SystemTime {
        let mut times = self.times.lock().expect("test clock poisoned");
        if times.len() > 1 {
            times.remove(0)
        } else {
            times[0]
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct RecordingLogger {
    pub(super) logs: Mutex<Vec<String>>,
}

impl Logger for RecordingLogger {
    fn debug(&self, args: fmt::Arguments<'_>) {
        self.logs.lock().unwrap().push(args.to_string());
    }

    fn info(&self, args: fmt::Arguments<'_>) {
        self.logs.lock().unwrap().push(args.to_string());
    }

    fn warn(&self, args: fmt::Arguments<'_>) {
        self.logs.lock().unwrap().push(args.to_string());
    }

    fn error(&self, args: fmt::Arguments<'_>) {
        self.logs.lock().unwrap().push(args.to_string());
    }

    fn fatal(&self, args: fmt::Arguments<'_>) {
        self.logs.lock().unwrap().push(args.to_string());
    }
}

pub(super) fn message(id: &str) -> TaskMessage {
    let mut message = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    message.id = id.to_owned();
    message.queue = "critical".to_owned();
    message.retry = 3;
    message
}

pub(super) fn dequeued(message: TaskMessage) -> DequeuedTask {
    DequeuedTask::new(message, UNIX_EPOCH + Duration::from_secs(1_700_000_030))
}

pub(super) fn dequeued_with_lease(
    message: TaskMessage,
    lease_expires_at: SystemTime,
) -> DequeuedTask {
    DequeuedTask::new(message, lease_expires_at)
}

#[derive(Clone)]
pub(super) struct CountingRetryDelay {
    calls: Arc<AtomicUsize>,
    delay: Duration,
}

impl CountingRetryDelay {
    pub(super) fn new(delay: Duration) -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
            delay,
        }
    }

    pub(super) fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

impl RetryDelay for CountingRetryDelay {
    fn retry_delay(&mut self, _retried: i32, _error: &HandlerError, _task: &Task) -> Duration {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.delay
    }
}

pub(super) struct PendingHandler {
    pub(super) calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Handler for PendingHandler {
    async fn process_task(
        &mut self,
        _task: &Task,
        _context: &ProcessingContext,
    ) -> Result<(), HandlerError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        std::future::pending::<()>().await;
        Ok(())
    }
}

pub(super) struct SleepingHandler {
    pub(super) duration: Duration,
}

#[async_trait]
impl Handler for SleepingHandler {
    async fn process_task(
        &mut self,
        _task: &Task,
        _context: &ProcessingContext,
    ) -> Result<(), HandlerError> {
        tokio::time::sleep(self.duration).await;
        Ok(())
    }
}
