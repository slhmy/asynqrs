use super::*;

#[tokio::test]
async fn lease_extension_error_stops_processing() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let message = message("task-id");
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message))],
        extend_lease_error: Some(LeaseError::Other("lease failed".to_owned())),
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts_hooks_and_lease_extender(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        DefaultRetryDelay,
        TestClock(now),
        DefaultIsFailure,
        NoopErrorHandler,
        ExtendLeaseBeforeProcess,
    );

    let error = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap_err();

    assert_eq!(
        error,
        ProcessingError::Lease(LeaseError::Other("lease failed".to_owned()))
    );
    assert!(assembly.broker.completed.is_empty());
    assert_eq!(
        assembly.broker.lease_extensions,
        [("critical".to_owned(), "task-id".to_owned())]
    );
}

#[tokio::test]
async fn background_lease_extender_runs_while_handler_is_processing() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let message = message("task-id");
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message))],
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts_hooks_and_lease_extender(
        broker,
        SleepingHandler {
            duration: Duration::from_millis(20),
        },
        DefaultRetryDelay,
        TestClock(now),
        DefaultIsFailure,
        NoopErrorHandler,
        ExtendLeaseWhileProcessing::every(Duration::from_millis(5)),
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
    assert!(!assembly.broker.lease_extensions.is_empty());
    assert!(
        assembly
            .broker
            .lease_extensions
            .iter()
            .all(|extension| extension == &("critical".to_owned(), "task-id".to_owned()))
    );
}

#[tokio::test]
async fn background_lease_extension_stops_after_handler_returns() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let message = message("task-id");
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message))],
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts_hooks_and_lease_extender(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        DefaultRetryDelay,
        TestClock(now),
        DefaultIsFailure,
        NoopErrorHandler,
        ExtendLeaseWhileProcessing::every(Duration::from_millis(5)),
    );

    let result = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();
    let extensions_after_return = assembly.broker.lease_extensions.len();
    tokio::time::sleep(Duration::from_millis(15)).await;

    assert_eq!(
        result,
        WorkerRun::Completed {
            task_id: "task-id".to_owned()
        }
    );
    assert_eq!(
        assembly.broker.lease_extensions.len(),
        extensions_after_return
    );
}

#[tokio::test]
async fn background_lease_extension_error_stops_processing() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let message = message("task-id");
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message))],
        extend_lease_error: Some(LeaseError::Other("lease failed".to_owned())),
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts_hooks_and_lease_extender(
        broker,
        PendingHandler {
            calls: Arc::new(AtomicUsize::new(0)),
        },
        DefaultRetryDelay,
        TestClock(now),
        DefaultIsFailure,
        NoopErrorHandler,
        ExtendLeaseWhileProcessing::every(Duration::from_millis(5)),
    );

    let error = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap_err();

    assert_eq!(
        error,
        ProcessingError::Lease(LeaseError::Other("lease failed".to_owned()))
    );
    assert!(assembly.broker.completed.is_empty());
    assert!(assembly.broker.retried.is_empty());
    assert!(assembly.broker.archived.is_empty());
    assert_eq!(
        assembly.broker.lease_extensions,
        [("critical".to_owned(), "task-id".to_owned())]
    );
}
