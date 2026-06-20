use super::*;

#[tokio::test]
async fn reports_idle_without_error() {
    let logger = Arc::new(RecordingLogger::default());
    let runtime_logger: Arc<dyn Logger> = logger.clone();
    let broker = RecordingBroker {
        dequeued: vec![Err(DequeueError::NoProcessableTask)],
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        DefaultRetryDelay,
        SystemClock,
    )
    .with_logger(runtime_logger)
    .with_log_level(LogLevel::Debug);

    let result = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(result, WorkerRun::NoProcessableTask);
    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        ["All queues are empty"]
    );
}

#[tokio::test]
async fn dequeue_errors_report_no_processed_task() {
    let logger = Arc::new(RecordingLogger::default());
    let runtime_logger: Arc<dyn Logger> = logger.clone();
    let broker = RecordingBroker {
        dequeued: vec![Err(DequeueError::Other("redis down".to_owned()))],
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        DefaultRetryDelay,
        SystemClock,
    )
    .with_logger(runtime_logger);

    let result = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(result, WorkerRun::NoProcessableTask);
    assert!(assembly.broker.completed.is_empty());
    assert!(assembly.broker.retried.is_empty());
    assert!(assembly.broker.archived.is_empty());
    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        ["Dequeue error: redis down"]
    );
}

#[tokio::test]
async fn dequeue_error_logs_are_rate_limited_like_upstream() {
    let logger = Arc::new(RecordingLogger::default());
    let runtime_logger: Arc<dyn Logger> = logger.clone();
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let broker = RecordingBroker {
        dequeued: vec![
            Err(DequeueError::Other("redis down 1".to_owned())),
            Err(DequeueError::Other("redis down 2".to_owned())),
            Err(DequeueError::Other("redis down 3".to_owned())),
        ],
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        DefaultRetryDelay,
        SequenceClock::new([
            now,
            now + Duration::from_secs(2),
            now + Duration::from_secs(3),
        ]),
    )
    .with_logger(runtime_logger);

    let queues = ["critical".to_owned()];
    run_worker_once(&mut assembly, &queues).await.unwrap();
    run_worker_once(&mut assembly, &queues).await.unwrap();
    run_worker_once(&mut assembly, &queues).await.unwrap();

    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        ["Dequeue error: redis down 1", "Dequeue error: redis down 3"]
    );
}

#[tokio::test]
async fn handler_panic_is_retried_like_handler_failure() {
    let logger = Arc::new(RecordingLogger::default());
    let runtime_logger: Arc<dyn Logger> = logger.clone();
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message("task-id")))],
        ..RecordingBroker::default()
    };
    let observed_error = Arc::new(Mutex::new(None));
    let observed_error_probe = Arc::clone(&observed_error);
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| -> Result<(), HandlerError> { panic!("boom") },
        move |_retried, error: &HandlerError, _task: &Task| {
            *observed_error_probe
                .lock()
                .expect("observed error poisoned") = Some(error.clone());
            Duration::from_secs(60)
        },
        TestClock(UNIX_EPOCH),
    )
    .with_logger(runtime_logger);

    let result = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        result,
        WorkerRun::Retried {
            task_id: "task-id".to_owned(),
            retry_at: UNIX_EPOCH + Duration::from_secs(60),
        }
    );
    assert!(assembly.broker.completed.is_empty());
    assert_eq!(assembly.broker.retried.len(), 1);
    let retried = &assembly.broker.retried[0];
    assert_eq!(retried.0, "task-id");
    assert_eq!(retried.1, UNIX_EPOCH + Duration::from_secs(60));
    assert!(
        retried.2.starts_with("panic ["),
        "panic error should include source location: {}",
        retried.2
    );
    assert!(retried.2.ends_with(": boom"));
    assert!(retried.3);
    assert!(assembly.broker.archived.is_empty());
    let observed_error = observed_error
        .lock()
        .expect("observed error poisoned")
        .clone()
        .unwrap();
    assert!(is_panic_error(&observed_error));
    assert!(is_panic_error(&observed_error));
    assert!(observed_error.is_panic());
    assert!(observed_error.to_string().starts_with("panic ["));
    assert!(observed_error.to_string().ends_with(": boom"));
    let logs = logger.logs.lock().unwrap();
    assert_eq!(logs.len(), 1);
    assert!(logs[0].starts_with("recovering from panic. See the stack trace below for details:\n"));
}
