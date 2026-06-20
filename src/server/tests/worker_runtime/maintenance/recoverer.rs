use super::*;

#[tokio::test]
async fn recovery_uses_is_failure_hook_for_retry_stats() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let broker = RecordingBroker {
        lease_expired: vec![message("retry-1")],
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        |_retried: i32, _error: &HandlerError, _task: &Task| Duration::from_secs(90),
        TestClock(now),
    )
    .with_is_failure(|error: &HandlerError| error.to_string() != "asynq: task lease expired");

    let result = assembly
        .run_maintenance(&["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(result.recovered_retried, 1);
    assert_eq!(
        assembly.broker.lease_expired_cutoffs,
        [now - Duration::from_secs(30)]
    );
    assert_eq!(
        assembly.broker.retried,
        [(
            "retry-1".to_owned(),
            now + Duration::from_secs(90),
            "asynq: task lease expired".to_owned(),
            false
        )]
    );
}

#[tokio::test]
async fn maintenance_reclaims_stale_aggregation_sets_per_queue() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let broker = RecordingBroker {
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
        .run_maintenance(&["critical".to_owned(), "default".to_owned()])
        .await
        .unwrap();

    assert_eq!(result.deleted_expired_completed, 10);
    assert_eq!(
        assembly.broker.reclaimed_stale_aggregation_sets,
        [("critical".to_owned(), now), ("default".to_owned(), now)]
    );
}

#[tokio::test]
async fn recoverer_maintenance_reclaims_after_recovering_all_queues() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut archived = message("archive-1");
    archived.queue = "default".to_owned();
    archived.retry = 1;
    archived.retried = 1;
    let broker = RecordingBroker {
        lease_expired: vec![message("retry-1"), archived],
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        |_retried: i32, _error: &HandlerError, _task: &Task| Duration::from_secs(90),
        TestClock(now),
    );

    let result = assembly
        .run_recoverer_maintenance(&["critical".to_owned(), "default".to_owned()])
        .await
        .unwrap();

    assert_eq!(result.recovered_retried, 1);
    assert_eq!(result.recovered_archived, 1);
    assert_eq!(
        assembly.broker.maintenance_events,
        [
            "recover:critical,default".to_owned(),
            "reclaim:critical".to_owned(),
            "reclaim:default".to_owned()
        ]
    );
}
