use super::*;

#[tokio::test]
async fn reports_lease_expired_after_success_without_lifecycle_write() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let message = message("task-id");
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued_with_lease(
            message,
            now - Duration::from_secs(1),
        ))],
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        DefaultRetryDelay,
        TestClock(now),
    );

    let result = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        result,
        WorkerRun::LeaseExpired {
            task_id: "task-id".to_owned()
        }
    );
    assert!(assembly.broker.completed.is_empty());
}

#[tokio::test]
async fn already_expired_lease_interrupts_pending_handler() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let message = message("task-id");
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued_with_lease(
            message,
            now - Duration::from_secs(1),
        ))],
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        PendingHandler {
            calls: Arc::new(AtomicUsize::new(0)),
        },
        |_retried, _error: &HandlerError, _task: &Task| {
            panic!("retry delay should not run after the worker lease expires")
        },
        TestClock(now),
    );

    let result = tokio::time::timeout(
        Duration::from_millis(100),
        run_worker_once(&mut assembly, &["critical".to_owned()]),
    )
    .await
    .expect("expired lease should interrupt pending handler")
    .unwrap();

    assert_eq!(
        result,
        WorkerRun::LeaseExpired {
            task_id: "task-id".to_owned()
        }
    );
    assert!(assembly.broker.completed.is_empty());
    assert!(assembly.broker.retried.is_empty());
    assert!(assembly.broker.archived.is_empty());
}

#[tokio::test]
async fn skips_retry_after_lease_expires() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let message = message("task-id");
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued_with_lease(
            message,
            now - Duration::from_secs(1),
        ))],
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Err(HandlerError::failed("boom")),
        |_retried, _error: &HandlerError, _task: &Task| {
            panic!("retry delay should not run after the worker lease expires")
        },
        TestClock(now),
    );

    let result = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        result,
        WorkerRun::LeaseExpired {
            task_id: "task-id".to_owned()
        }
    );
    assert!(assembly.broker.retried.is_empty());
}

#[tokio::test]
async fn terminal_failures_report_lease_expired_without_lifecycle_writes() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut exhausted = message("exhausted-id");
    exhausted.retry = 1;
    exhausted.retried = 1;
    let skipped = message("skipped-id");
    let revoked = message("revoked-id");
    let broker = RecordingBroker {
        dequeued: vec![
            Ok(dequeued_with_lease(exhausted, now - Duration::from_secs(1))),
            Ok(dequeued_with_lease(skipped, now - Duration::from_secs(1))),
            Ok(dequeued_with_lease(revoked, now - Duration::from_secs(1))),
        ],
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        {
            let mut calls = 0;
            move |_task: &Task| {
                calls += 1;
                match calls {
                    1 => Err(HandlerError::failed("exhausted")),
                    2 => Err(HandlerError::skip_retry("skip")),
                    _ => Err(HandlerError::revoke_task("revoke")),
                }
            }
        },
        |_retried, _error: &HandlerError, _task: &Task| {
            panic!("retry delay should not run after the worker lease expires")
        },
        TestClock(now),
    );

    let first = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();
    let second = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();
    let third = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        first,
        WorkerRun::LeaseExpired {
            task_id: "exhausted-id".to_owned()
        }
    );
    assert_eq!(
        second,
        WorkerRun::LeaseExpired {
            task_id: "skipped-id".to_owned()
        }
    );
    assert_eq!(
        third,
        WorkerRun::LeaseExpired {
            task_id: "revoked-id".to_owned()
        }
    );
    assert!(assembly.broker.completed.is_empty());
    assert!(assembly.broker.retried.is_empty());
    assert!(assembly.broker.archived.is_empty());
}

#[tokio::test]
async fn lease_expiration_during_handler_retries_without_broker_write() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let message = message("task-id");
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued_with_lease(
            message,
            now + Duration::from_millis(5),
        ))],
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        SleepingHandler {
            duration: Duration::from_millis(25),
        },
        |_retried, _error: &HandlerError, _task: &Task| {
            panic!("retry delay should not run after the worker lease expires")
        },
        TestClock(now),
    );

    let result = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        result,
        WorkerRun::LeaseExpired {
            task_id: "task-id".to_owned()
        }
    );
    assert!(assembly.broker.completed.is_empty());
    assert!(assembly.broker.retried.is_empty());
    assert!(assembly.broker.archived.is_empty());
}

#[test]
fn lease_expired_error_predicate_matches_upstream_sentinel() {
    let expired = lease_expired_error();
    let failed = HandlerError::failed("asynq: task lease expired");

    assert_eq!(expired, HandlerError::LeaseExpired);
    assert_eq!(expired.to_string(), "asynq: task lease expired");
    assert!(is_lease_expired_error(&expired));
    assert!(expired.is_lease_expired());
    assert!(!is_lease_expired_error(&failed));
}
