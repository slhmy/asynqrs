use super::*;

#[tokio::test]
async fn canceller_retries_active_task() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let message = message("task-id");
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message))],
        ..RecordingBroker::default()
    };
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        SelfCancellingPendingHandler {
            calls: Arc::clone(&handler_calls),
            canceller: ServerCanceller::default(),
            task_id: "task-id".to_owned(),
        },
        |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
        TestClock(now),
    );
    let canceller = assembly.canceller();
    assembly.executor.handler.canceller = canceller.clone();

    let result = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        result,
        WorkerRun::Retried {
            task_id: "task-id".to_owned(),
            retry_at: now + Duration::from_secs(60),
        }
    );
    assert_eq!(
        assembly.broker.retried,
        [(
            "task-id".to_owned(),
            now + Duration::from_secs(60),
            "context canceled".to_owned(),
            true
        )]
    );
    assert_eq!(handler_calls.load(Ordering::Relaxed), 1);
    assert!(!canceller.cancel("task-id"));
    assert!(assembly.broker.completed.is_empty());
    assert!(assembly.broker.archived.is_empty());
}

#[tokio::test]
async fn handler_can_observe_task_context_deadline_and_cancellation() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut message = message("task-id");
    message.deadline = 1_700_000_060;
    message.retry = 7;
    message.retried = 2;
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message))],
        ..RecordingBroker::default()
    };
    let saw_deadline = Arc::new(AtomicUsize::new(0));
    let saw_cancelled = Arc::new(AtomicUsize::new(0));
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        ContextObservingHandler {
            saw_deadline: Arc::clone(&saw_deadline),
            saw_cancelled: Arc::clone(&saw_cancelled),
            canceller: ServerCanceller::default(),
            task_id: "task-id".to_owned(),
            queue_name: "critical".to_owned(),
            retry_count: 2,
            max_retry: 7,
            expected_deadline: UNIX_EPOCH + Duration::from_secs(1_700_000_060),
        },
        |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
        TestClock(now),
    );
    let canceller = assembly.canceller();
    assembly.executor.handler.canceller = canceller;

    let result = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        result,
        WorkerRun::Retried {
            task_id: "task-id".to_owned(),
            retry_at: now + Duration::from_secs(60),
        }
    );
    assert_eq!(saw_deadline.load(Ordering::Relaxed), 1);
    assert_eq!(saw_cancelled.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn result_writer_rejects_write_after_task_cancellation() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let message = message("task-id");
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message))],
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        SelfCancellingResultWriterHandler {
            canceller: ServerCanceller::default(),
            task_id: "task-id".to_owned(),
        },
        DefaultRetryDelay,
        TestClock(now),
    );
    let canceller = assembly.canceller();
    assembly.executor.handler.canceller = canceller;

    let result = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        result,
        WorkerRun::Completed {
            task_id: "task-id".to_owned()
        }
    );
    assert!(assembly.broker.results.is_empty());
    assert_eq!(assembly.broker.completed, ["task-id"]);
}

#[tokio::test]
async fn result_writer_rejects_write_after_task_deadline() {
    let now = UNIX_EPOCH + Duration::new(1_700_000_000, 999_000_000);
    let mut message = message("task-id");
    message.deadline = 1_700_000_001;
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message))],
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        ExpiredDeadlineResultWriterHandler,
        DefaultRetryDelay,
        TestClock(now),
    );

    let result = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        result,
        WorkerRun::Completed {
            task_id: "task-id".to_owned()
        }
    );
    assert!(assembly.broker.results.is_empty());
    assert_eq!(assembly.broker.completed, ["task-id"]);
}

struct SelfCancellingPendingHandler {
    calls: Arc<AtomicUsize>,
    canceller: ServerCanceller,
    task_id: String,
}

struct ContextObservingHandler {
    saw_deadline: Arc<AtomicUsize>,
    saw_cancelled: Arc<AtomicUsize>,
    canceller: ServerCanceller,
    task_id: String,
    queue_name: String,
    retry_count: i32,
    max_retry: i32,
    expected_deadline: SystemTime,
}

struct SelfCancellingResultWriterHandler {
    canceller: ServerCanceller,
    task_id: String,
}

struct ExpiredDeadlineResultWriterHandler;

#[async_trait]
impl Handler for ContextObservingHandler {
    async fn process_task(
        &mut self,
        _task: &Task,
        context: &ProcessingContext,
    ) -> Result<(), HandlerError> {
        if context.deadline() == Some(self.expected_deadline) {
            self.saw_deadline.fetch_add(1, Ordering::Relaxed);
        }
        assert_eq!(context.task_id(), self.task_id.as_str());
        assert_eq!(context.queue_name(), self.queue_name.as_str());
        assert_eq!(context.retry_count(), self.retry_count);
        assert_eq!(context.max_retry(), self.max_retry);
        assert!(!context.is_cancelled());
        assert!(self.canceller.cancel(&self.task_id));
        if context.is_cancelled() {
            self.saw_cancelled.fetch_add(1, Ordering::Relaxed);
        }
        std::future::pending::<()>().await;
        Ok(())
    }
}

#[async_trait]
impl Handler for SelfCancellingPendingHandler {
    async fn process_task(
        &mut self,
        _task: &Task,
        _context: &ProcessingContext,
    ) -> Result<(), HandlerError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        assert!(self.canceller.cancel(&self.task_id));
        std::future::pending::<()>().await;
        Ok(())
    }
}

#[async_trait]
impl Handler for SelfCancellingResultWriterHandler {
    async fn process_task(
        &mut self,
        _task: &Task,
        context: &ProcessingContext,
    ) -> Result<(), HandlerError> {
        assert!(self.canceller.cancel(&self.task_id));

        let error = context
            .write_result(b"handler-result".to_vec())
            .unwrap_err();

        assert_eq!(
            error,
            crate::ResultError::WriteFailed("context canceled".to_owned())
        );
        assert_eq!(
            error.to_string(),
            "failed to write task result: context canceled"
        );
        Ok(())
    }
}

#[async_trait]
impl Handler for ExpiredDeadlineResultWriterHandler {
    async fn process_task(
        &mut self,
        _task: &Task,
        context: &ProcessingContext,
    ) -> Result<(), HandlerError> {
        std::thread::sleep(Duration::from_millis(10));

        let error = context
            .write_result(b"handler-result".to_vec())
            .unwrap_err();

        assert_eq!(
            error,
            crate::ResultError::WriteFailed("context deadline exceeded".to_owned())
        );
        assert_eq!(
            error.to_string(),
            "failed to write task result: context deadline exceeded"
        );
        Ok(())
    }
}
