use super::*;

#[tokio::test]
async fn run_once_ignores_group_list_error_without_checking_groups() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let aggregator_logger: Arc<dyn Logger> = logger.clone();
    let broker = RecordingAggregationBroker {
        groups: vec!["tenant-a".to_owned()],
        group_list_error: Some(AggregationError::Other("scan failed".to_owned())),
        ..RecordingAggregationBroker::default()
    };
    let mut aggregator = Aggregator::with_clock(
        broker,
        RecordingAggregationHandler::default(),
        TestClock(now),
    )
    .with_test_logger(aggregator_logger)
    .with_log_level(LogLevel::Unspecified);
    aggregator.add_auto_group_config(
        AggregationGroupConfig::new(
            "critical",
            Duration::from_secs(10),
            Duration::from_secs(60),
            2,
        )
        .unwrap(),
    );

    let run = run_aggregator_once(&mut aggregator).await.unwrap();

    assert_eq!(run.checked, 0);
    assert_eq!(run.aggregated, 0);
    assert_eq!(run.reclaimed, 1);
    let broker = &aggregator.broker;
    assert_eq!(broker.group_lists, ["critical"]);
    assert!(broker.checks.is_empty());
    assert!(broker.reads.is_empty());
    assert!(broker.enqueued.is_empty());
    assert!(broker.deletes.is_empty());
    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        ["Failed to list groups in queue: \"critical\""]
    );
}

#[tokio::test]
async fn run_once_ignores_reclaim_error_for_configured_group() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let aggregator_logger: Arc<dyn Logger> = logger.clone();
    let broker = RecordingAggregationBroker {
        reclaim_error: Some(AggregationError::Other("reclaim failed".to_owned())),
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

    assert_eq!(run.reclaimed, 0);
    assert_eq!(run.checked, 1);
    let broker = &aggregator.broker;
    assert_eq!(broker.reclaims, [("critical".to_owned(), now)]);
    assert_eq!(broker.checks.len(), 1);
    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        [
            "recoverer: could not reclaim stale aggregation sets in queue \"critical\": reclaim failed"
        ]
    );
}

#[tokio::test]
async fn run_once_ignores_reclaim_error_for_auto_group_discovery() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let broker = RecordingAggregationBroker {
        groups: vec!["tenant-a".to_owned()],
        reclaim_error: Some(AggregationError::Other("reclaim failed".to_owned())),
        ..RecordingAggregationBroker::default()
    };
    let mut aggregator = Aggregator::with_clock(
        broker,
        RecordingAggregationHandler::default(),
        TestClock(now),
    );
    aggregator.add_auto_group_config(
        AggregationGroupConfig::new(
            "critical",
            Duration::from_secs(10),
            Duration::from_secs(60),
            2,
        )
        .unwrap(),
    );

    let run = run_aggregator_once(&mut aggregator).await.unwrap();

    assert_eq!(run.reclaimed, 0);
    assert_eq!(run.checked, 1);
    let broker = &aggregator.broker;
    assert_eq!(broker.reclaims, [("critical".to_owned(), now)]);
    assert_eq!(broker.group_lists, ["critical"]);
    assert_eq!(broker.checks.len(), 1);
}
