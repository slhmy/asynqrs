use super::*;

#[tokio::test]
async fn task_timeout_retries_when_handler_exceeds_timeout() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut message = message("task-id");
    message.timeout = 1;
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message))],
        ..RecordingBroker::default()
    };
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        PendingHandler {
            calls: Arc::clone(&handler_calls),
        },
        |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
        TestClock(now),
    );

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        run_worker_once(&mut assembly, &["critical".to_owned()]),
    )
    .await
    .unwrap()
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
            "context deadline exceeded".to_owned(),
            true
        )]
    );
    assert_eq!(handler_calls.load(Ordering::Relaxed), 1);
    assert!(assembly.broker.completed.is_empty());
    assert!(assembly.broker.archived.is_empty());
}

#[tokio::test]
async fn timeout_and_cancellation_use_configured_retry_delay() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut timeout_message = message("timeout-id");
    timeout_message.timeout = 1;
    let cancel_message = message("cancel-id");
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(timeout_message)), Ok(dequeued(cancel_message))],
        ..RecordingBroker::default()
    };
    let retry_delay = CountingRetryDelay::new(Duration::from_secs(45));
    let retry_delay_probe = retry_delay.clone();
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        TimeoutThenCancelHandler { calls: 0 },
        retry_delay,
        TestClock(now),
    );

    let first = tokio::time::timeout(
        Duration::from_secs(2),
        run_worker_once(&mut assembly, &["critical".to_owned()]),
    )
    .await
    .unwrap()
    .unwrap();
    let second = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        first,
        WorkerRun::Retried {
            task_id: "timeout-id".to_owned(),
            retry_at: now + Duration::from_secs(45),
        }
    );
    assert_eq!(
        second,
        WorkerRun::Retried {
            task_id: "cancel-id".to_owned(),
            retry_at: now + Duration::from_secs(45),
        }
    );
    assert_eq!(retry_delay_probe.calls(), 2);
}

struct TimeoutThenCancelHandler {
    calls: usize,
}

#[async_trait]
impl Handler for TimeoutThenCancelHandler {
    async fn process_task(
        &mut self,
        _task: &Task,
        _context: &ProcessingContext,
    ) -> Result<(), HandlerError> {
        self.calls += 1;
        if self.calls == 1 {
            std::future::pending::<()>().await;
            Ok(())
        } else {
            Err(HandlerError::failed("context canceled"))
        }
    }
}
