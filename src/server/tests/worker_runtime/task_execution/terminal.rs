use super::*;

#[tokio::test]
async fn archives_when_retry_is_exhausted_or_skipped() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let runtime_logger: Arc<dyn Logger> = logger.clone();
    let mut exhausted = message("exhausted-id");
    exhausted.retry = 1;
    exhausted.retried = 1;
    let skipped = message("skipped-id");
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(exhausted)), Ok(dequeued(skipped))],
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        {
            let mut calls = 0;
            move |_task: &Task| {
                calls += 1;
                if calls == 1 {
                    Err(HandlerError::failed("exhausted"))
                } else {
                    Err(HandlerError::skip_retry("skip"))
                }
            }
        },
        |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
        TestClock(now),
    )
    .with_logger(runtime_logger);

    let first = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();
    let second = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        first,
        WorkerRun::Archived {
            task_id: "exhausted-id".to_owned()
        }
    );
    assert_eq!(
        second,
        WorkerRun::Archived {
            task_id: "skipped-id".to_owned()
        }
    );
    assert_eq!(assembly.broker.archived.len(), 2);
    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        [
            "Retry exhausted for task id=exhausted-id",
            "Retry exhausted for task id=skipped-id",
        ]
    );
}

#[tokio::test]
async fn archive_errors_do_not_fail_terminal_failed_tasks() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let runtime_logger: Arc<dyn Logger> = logger.clone();
    let mut exhausted = message("exhausted-id");
    exhausted.retry = 1;
    exhausted.retried = 1;
    let skipped = message("skipped-id");
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(exhausted)), Ok(dequeued(skipped))],
        archive_error: Some(ArchiveError::Other("redis down".to_owned())),
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        {
            let mut calls = 0;
            move |_task: &Task| {
                calls += 1;
                if calls == 1 {
                    Err(HandlerError::failed("exhausted"))
                } else {
                    Err(HandlerError::skip_retry("skip"))
                }
            }
        },
        |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
        TestClock(now),
    )
    .with_logger(runtime_logger);

    let first = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();
    let second = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        first,
        WorkerRun::Archived {
            task_id: "exhausted-id".to_owned()
        }
    );
    assert_eq!(
        second,
        WorkerRun::Archived {
            task_id: "skipped-id".to_owned()
        }
    );
    assert_eq!(assembly.broker.archived.len(), 3);
    assert_eq!(assembly.pending_sync_count(), 2);
    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        [
            "Retry exhausted for task id=exhausted-id",
            "Could not move task id=exhausted-id from \"asynq:{critical}:active\" to \"asynq:{critical}:archived\"; Will retry syncing",
            "Retry exhausted for task id=skipped-id",
            "Could not move task id=skipped-id from \"asynq:{critical}:active\" to \"asynq:{critical}:archived\"; Will retry syncing",
        ]
    );
}

#[tokio::test]
async fn retry_delay_is_not_called_for_terminal_handler_errors() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let runtime_logger: Arc<dyn Logger> = logger.clone();
    let skipped = message("skipped-id");
    let revoked = message("revoked-id");
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(skipped)), Ok(dequeued(revoked))],
        ..RecordingBroker::default()
    };
    let retry_delay = CountingRetryDelay::new(Duration::from_secs(60));
    let retry_delay_probe = retry_delay.clone();
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        {
            let mut calls = 0;
            move |_task: &Task| {
                calls += 1;
                if calls == 1 {
                    Err(HandlerError::skip_retry("skip"))
                } else {
                    Err(HandlerError::revoke_task("revoke"))
                }
            }
        },
        retry_delay,
        TestClock(now),
    )
    .with_logger(runtime_logger);

    let first = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();
    let second = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        first,
        WorkerRun::Archived {
            task_id: "skipped-id".to_owned()
        }
    );
    assert_eq!(
        second,
        WorkerRun::Revoked {
            task_id: "revoked-id".to_owned()
        }
    );
    assert_eq!(retry_delay_probe.calls(), 0);
    assert!(assembly.broker.retried.is_empty());
    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        [
            "Retry exhausted for task id=skipped-id",
            "revoke task id=revoked-id",
        ]
    );
}

#[test]
fn terminal_handler_error_predicates_match_upstream_sentinels() {
    let skipped = HandlerError::skip_retry("skip");
    let revoked = HandlerError::revoke_task("revoke");
    let failed = HandlerError::failed("boom");

    assert!(is_skip_retry_error(&skipped));
    assert!(skipped.is_skip_retry());
    assert!(!is_skip_retry_error(&failed));

    assert!(is_revoke_task_error(&revoked));
    assert!(revoked.is_revoke_task());
    assert!(!is_revoke_task_error(&failed));
}

#[test]
fn terminal_handler_error_helpers_match_upstream_sentinel_messages() {
    let skipped = HandlerError::skip_retry("skip retry for the task");
    let revoked = HandlerError::revoke_task("revoke task");

    assert!(is_skip_retry_error(&skipped));
    assert_eq!(skipped.to_string(), "skip retry for the task");

    assert!(is_revoke_task_error(&revoked));
    assert_eq!(revoked.to_string(), "revoke task");
}

#[tokio::test]
async fn complete_errors_do_not_fail_revoked_task() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let runtime_logger: Arc<dyn Logger> = logger.clone();
    let message = message("revoked-id");
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message))],
        complete_error: Some(CompleteError::Other("redis down".to_owned())),
        ..RecordingBroker::default()
    };
    let retry_delay = CountingRetryDelay::new(Duration::from_secs(60));
    let retry_delay_probe = retry_delay.clone();
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Err(HandlerError::revoke_task("revoke")),
        retry_delay,
        TestClock(now),
    )
    .with_logger(runtime_logger);

    let result = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        result,
        WorkerRun::Revoked {
            task_id: "revoked-id".to_owned()
        }
    );
    assert_eq!(retry_delay_probe.calls(), 0);
    assert_eq!(assembly.broker.completed, ["revoked-id"]);
    assert!(assembly.broker.retried.is_empty());
    assert!(assembly.broker.archived.is_empty());
    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        [
            "revoke task id=revoked-id",
            "Could not remove task id=revoked-id type=\"email:welcome\" from \"asynq:{critical}:active\" err: redis down; Will retry syncing"
        ]
    );
}
