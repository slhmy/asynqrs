use super::super::*;

#[tokio::test]
async fn pending_complete_sync_retries_before_next_dequeue() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let message = message("task-id");
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message)), Err(DequeueError::NoProcessableTask)],
        complete_error: Some(CompleteError::Other("redis down".to_owned())),
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        DefaultRetryDelay,
        TestClock(now),
    );

    let first = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();
    assembly.broker_mut().complete_error = None;
    let second = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        first,
        WorkerRun::Completed {
            task_id: "task-id".to_owned()
        }
    );
    assert_eq!(second, WorkerRun::NoProcessableTask);
    assert_eq!(assembly.broker.completed, ["task-id", "task-id"]);
    assert_eq!(assembly.pending_sync_count(), 0);
}

#[tokio::test]
async fn stale_pending_complete_sync_is_dropped_after_lease_deadline() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let lease_deadline = now + Duration::from_secs(30);
    let message = message("task-id");
    let broker = RecordingBroker {
        dequeued: vec![
            Ok(dequeued_with_lease(message, lease_deadline)),
            Err(DequeueError::NoProcessableTask),
        ],
        complete_error: Some(CompleteError::Other("redis down".to_owned())),
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        DefaultRetryDelay,
        SequenceClock::new([now, now, lease_deadline + Duration::from_nanos(1)]),
    );

    let first = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();
    assembly.broker_mut().complete_error = None;
    let second = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        first,
        WorkerRun::Completed {
            task_id: "task-id".to_owned()
        }
    );
    assert_eq!(second, WorkerRun::NoProcessableTask);
    assert_eq!(assembly.broker.completed, ["task-id"]);
    assert_eq!(assembly.pending_sync_count(), 0);
}

#[tokio::test]
async fn pending_retry_sync_retries_before_next_dequeue() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let message = message("task-id");
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message)), Err(DequeueError::NoProcessableTask)],
        retry_error: Some(RetryError::Other("redis down".to_owned())),
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Err(HandlerError::failed("boom")),
        |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
        TestClock(now),
    );

    let first = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();
    assembly.broker_mut().retry_error = None;
    let second = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        first,
        WorkerRun::Retried {
            task_id: "task-id".to_owned(),
            retry_at: now + Duration::from_secs(60)
        }
    );
    assert_eq!(second, WorkerRun::NoProcessableTask);
    assert_eq!(assembly.broker.retried.len(), 2);
    assert_eq!(assembly.pending_sync_count(), 0);
}

#[tokio::test]
async fn stale_pending_retry_sync_is_dropped_after_lease_deadline() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let lease_deadline = now + Duration::from_secs(30);
    let message = message("task-id");
    let broker = RecordingBroker {
        dequeued: vec![
            Ok(dequeued_with_lease(message, lease_deadline)),
            Err(DequeueError::NoProcessableTask),
        ],
        retry_error: Some(RetryError::Other("redis down".to_owned())),
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Err(HandlerError::failed("boom")),
        |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
        SequenceClock::new([now, now, lease_deadline + Duration::from_nanos(1)]),
    );

    let first = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();
    assembly.broker_mut().retry_error = None;
    let second = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        first,
        WorkerRun::Retried {
            task_id: "task-id".to_owned(),
            retry_at: now + Duration::from_secs(60)
        }
    );
    assert_eq!(second, WorkerRun::NoProcessableTask);
    assert_eq!(assembly.broker.retried.len(), 1);
    assert_eq!(assembly.pending_sync_count(), 0);
}

#[tokio::test]
async fn pending_archive_sync_retries_before_next_dequeue() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut message = message("task-id");
    message.retry = 1;
    message.retried = 1;
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message)), Err(DequeueError::NoProcessableTask)],
        archive_error: Some(ArchiveError::Other("redis down".to_owned())),
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Err(HandlerError::failed("boom")),
        |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
        TestClock(now),
    );

    let first = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();
    assembly.broker_mut().archive_error = None;
    let second = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        first,
        WorkerRun::Archived {
            task_id: "task-id".to_owned()
        }
    );
    assert_eq!(second, WorkerRun::NoProcessableTask);
    assert_eq!(assembly.broker.archived.len(), 2);
    assert_eq!(assembly.pending_sync_count(), 0);
}

#[tokio::test]
async fn stale_pending_archive_sync_is_dropped_after_lease_deadline() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let lease_deadline = now + Duration::from_secs(30);
    let mut message = message("task-id");
    message.retry = 1;
    message.retried = 1;
    let broker = RecordingBroker {
        dequeued: vec![
            Ok(dequeued_with_lease(message, lease_deadline)),
            Err(DequeueError::NoProcessableTask),
        ],
        archive_error: Some(ArchiveError::Other("redis down".to_owned())),
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Err(HandlerError::failed("boom")),
        |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
        SequenceClock::new([now, now, lease_deadline + Duration::from_nanos(1)]),
    );

    let first = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();
    assembly.broker_mut().archive_error = None;
    let second = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        first,
        WorkerRun::Archived {
            task_id: "task-id".to_owned()
        }
    );
    assert_eq!(second, WorkerRun::NoProcessableTask);
    assert_eq!(assembly.broker.archived.len(), 1);
    assert_eq!(assembly.pending_sync_count(), 0);
}

#[tokio::test]
async fn pending_sync_backlog_is_shared_by_assembly_clones() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let message = message("task-id");
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message))],
        complete_error: Some(CompleteError::Other("redis down".to_owned())),
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        DefaultRetryDelay,
        TestClock(now),
    );

    run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();
    assembly.broker_mut().complete_error = None;
    let mut syncer = assembly.clone();

    syncer.run_sync_once().await;

    assert_eq!(assembly.pending_sync_count(), 0);
    assert_eq!(syncer.pending_sync_count(), 0);
    assert_eq!(syncer.broker.completed, ["task-id", "task-id"]);
}

#[tokio::test]
async fn shutdown_retries_pending_sync_once_even_after_lease_deadline() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let lease_deadline = now + Duration::from_secs(30);
    let message = message("task-id");
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued_with_lease(message, lease_deadline))],
        complete_error: Some(CompleteError::Other("redis down".to_owned())),
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        DefaultRetryDelay,
        SequenceClock::new([now, now, lease_deadline + Duration::from_nanos(1)]),
    );

    run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();
    assembly.broker_mut().complete_error = None;
    assembly.shutdown().await.unwrap();

    assert_eq!(assembly.broker.completed, ["task-id", "task-id"]);
    assert_eq!(assembly.pending_sync_count(), 0);
}
