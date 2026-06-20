use super::*;

#[tokio::test]
async fn run_once_ignores_aggregation_check_error_without_reading_set() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let aggregator_logger: Arc<dyn Logger> = logger.clone();
    let broker = RecordingAggregationBroker {
        check_error: Some(AggregationError::Other("script failed".to_owned())),
        ..RecordingAggregationBroker::default()
    };
    let mut aggregator = Aggregator::with_clock(
        broker,
        RecordingAggregationHandler::default(),
        TestClock(now),
    )
    .with_test_logger(aggregator_logger);
    aggregator.add_test_group(
        AggregationGroup::new(
            "critical",
            "tenant-a",
            Duration::from_secs(10),
            Duration::from_secs(60),
            2,
        )
        .unwrap(),
    );

    let run = run_aggregator_once(&mut aggregator).await.unwrap();

    assert_eq!(run.checked, 0);
    assert_eq!(run.aggregated, 0);
    let broker = &aggregator.broker;
    assert_eq!(broker.checks.len(), 1);
    assert!(broker.reads.is_empty());
    assert!(broker.enqueued.is_empty());
    assert!(broker.deletes.is_empty());
    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        ["Failed to run aggregation check: queue=\"critical\" group=\"tenant-a\""]
    );
}

#[tokio::test]
async fn run_once_ignores_read_set_error_without_enqueueing() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let aggregator_logger: Arc<dyn Logger> = logger.clone();
    let broker = RecordingAggregationBroker {
        ready_set_id: Some("set-id".to_owned()),
        read_error: Some(AggregationError::Other("decode failed".to_owned())),
        ..RecordingAggregationBroker::default()
    };
    let mut aggregator = Aggregator::with_clock(
        broker,
        RecordingAggregationHandler::default(),
        TestClock(now),
    )
    .with_test_logger(aggregator_logger);
    aggregator.add_test_group(
        AggregationGroup::new(
            "critical",
            "tenant-a",
            Duration::from_secs(10),
            Duration::from_secs(60),
            2,
        )
        .unwrap(),
    );

    let run = run_aggregator_once(&mut aggregator).await.unwrap();

    assert_eq!(run.checked, 0);
    assert_eq!(run.aggregated, 0);
    let handler = Arc::clone(&aggregator.handler);
    let handler = handler.lock().await;
    assert!(handler.handled.is_empty());
    let broker = &aggregator.broker;
    assert_eq!(
        broker.reads,
        [(
            "critical".to_owned(),
            "tenant-a".to_owned(),
            "set-id".to_owned()
        )]
    );
    assert!(broker.enqueued.is_empty());
    assert!(broker.deletes.is_empty());
    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        [
            "Failed to read aggregation set: queue=\"critical\", group=\"tenant-a\", setID=\"set-id\""
        ]
    );
}
