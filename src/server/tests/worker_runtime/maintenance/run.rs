use super::*;

#[tokio::test]
async fn runs_maintenance() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let broker = RecordingBroker {
        forward_scheduled: 2,
        forward_retry: 1,
        lease_expired: vec![message("retry-1"), message("retry-2"), {
            let mut message = message("archive-1");
            message.retry = 1;
            message.retried = 1;
            message
        }],
        deleted_expired_completed: 5,
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        |_retried: i32, _error: &HandlerError, _task: &Task| Duration::from_secs(90),
        TestClock(now),
    );

    let result = assembly
        .run_maintenance(&["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(result.forwarded_scheduled(), 2);
    assert_eq!(result.forwarded_retry(), 1);
    assert_eq!(result.recovered_retried, 2);
    assert_eq!(result.recovered_archived, 1);
    assert_eq!(result.deleted_expired_completed, 5);
    assert_eq!(
        assembly.broker.forward_queue_calls,
        [vec!["critical".to_owned()]]
    );
    assert_eq!(
        assembly.broker.retried,
        [
            (
                "retry-1".to_owned(),
                now + Duration::from_secs(90),
                "asynq: task lease expired".to_owned(),
                true
            ),
            (
                "retry-2".to_owned(),
                now + Duration::from_secs(90),
                "asynq: task lease expired".to_owned(),
                true
            )
        ]
    );
    assert_eq!(
        assembly.broker.archived,
        [(
            "archive-1".to_owned(),
            "asynq: task lease expired".to_owned()
        )]
    );
    assert_eq!(
        assembly.broker.reclaimed_stale_aggregation_sets,
        [("critical".to_owned(), now)]
    );
    assert_eq!(
        assembly.broker.cleanup_batches,
        [("critical".to_owned(), DEFAULT_JANITOR_BATCH_SIZE)]
    );
}

#[tokio::test]
async fn maintenance_forward_and_cleanup_errors_do_not_fail_run() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let broker = RecordingBroker {
        forward_scheduled: 2,
        forward_scheduled_error: Some(ForwardError::Other("forward scheduled down".to_owned())),
        forward_retry: 1,
        forward_retry_error: Some(ForwardError::Other("forward retry down".to_owned())),
        lease_expired: vec![message("retry-1")],
        deleted_expired_completed: 5,
        delete_expired_completed_error: Some(CleanupError::Other("cleanup down".to_owned())),
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        |_retried: i32, _error: &HandlerError, _task: &Task| Duration::from_secs(90),
        TestClock(now),
    );

    let result = assembly
        .run_maintenance(&["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(result.forwarded_scheduled(), 0);
    assert_eq!(result.forwarded_retry(), 0);
    assert_eq!(result.recovered_retried, 1);
    assert_eq!(result.recovered_archived, 0);
    assert_eq!(result.deleted_expired_completed, 0);
    assert_eq!(assembly.broker.retried.len(), 1);
}
