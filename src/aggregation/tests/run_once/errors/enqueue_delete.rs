use super::*;

#[tokio::test]
async fn run_once_ignores_aggregated_enqueue_error_without_deleting_set() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let aggregator_logger: Arc<dyn Logger> = logger.clone();
    let broker = RecordingAggregationBroker {
        ready_set_id: Some("set-id".to_owned()),
        enqueue_error: Some(BrokerError::Other("redis down".to_owned())),
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
    assert_eq!(handler.handled.len(), 1);
    let broker = &aggregator.broker;
    assert!(broker.deletes.is_empty());
    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        [
            "Failed to enqueue aggregated task (queue=\"critical\", group=\"tenant-a\", setID=\"set-id\"): redis down"
        ]
    );
}

#[tokio::test]
async fn run_once_ignores_aggregated_enqueue_plan_error_without_deleting_set() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let broker = RecordingAggregationBroker {
        ready_set_id: Some("set-id".to_owned()),
        ..RecordingAggregationBroker::default()
    };
    let mut aggregator = Aggregator::with_clock(
        broker,
        RecordingAggregationHandler {
            task_type: Some(" ".to_owned()),
            ..RecordingAggregationHandler::default()
        },
        TestClock(now),
    );
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
    assert_eq!(handler.handled.len(), 1);
    let broker = &aggregator.broker;
    assert!(broker.enqueued.is_empty());
    assert!(broker.deletes.is_empty());
}

#[tokio::test]
async fn run_once_honors_expired_aggregation_deadline_before_enqueueing() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let aggregator_logger: Arc<dyn Logger> = logger.clone();
    let broker = RecordingAggregationBroker {
        ready_set_id: Some("set-id".to_owned()),
        aggregation_deadline: Some(now - Duration::from_secs(1)),
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
    // Reference: Asynq v0.26.0 calls `GroupAggregator.Aggregate` before
    // `Client.EnqueueContext` observes the aggregation-set deadline:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/aggregator.go#L119-L133>.
    assert_eq!(handler.handled.len(), 1);
    let broker = &aggregator.broker;
    assert!(broker.enqueued.is_empty());
    assert!(broker.deletes.is_empty());
    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        [
            "Failed to enqueue aggregated task (queue=\"critical\", group=\"tenant-a\", setID=\"set-id\"): context deadline exceeded"
        ]
    );
}

#[tokio::test]
async fn run_once_ignores_handler_error_without_enqueueing_or_deleting_set() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let aggregator_logger: Arc<dyn Logger> = logger.clone();
    let broker = RecordingAggregationBroker {
        ready_set_id: Some("set-id".to_owned()),
        ..RecordingAggregationBroker::default()
    };
    let mut aggregator = Aggregator::with_clock(
        broker,
        RecordingAggregationHandler {
            failures_before_success: 1,
            ..RecordingAggregationHandler::default()
        },
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
    aggregator.add_test_group(
        AggregationGroup::new(
            "critical",
            "tenant-b",
            Duration::from_secs(10),
            Duration::from_secs(60),
            2,
        )
        .unwrap(),
    );

    let run = run_aggregator_once(&mut aggregator).await.unwrap();

    assert_eq!(run.checked, 1);
    assert_eq!(run.aggregated, 1);
    let handler = Arc::clone(&aggregator.handler);
    let handler = handler.lock().await;
    assert_eq!(handler.handled.len(), 2);
    let broker = &aggregator.broker;
    assert_eq!(broker.enqueued.len(), 1);
    assert_eq!(broker.enqueued[0].message().queue, "critical");
    assert_eq!(
        broker.deletes,
        [(
            "critical".to_owned(),
            "tenant-b".to_owned(),
            "set-id".to_owned()
        )]
    );
    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        [
            "Failed to aggregate task (queue=\"critical\", group=\"tenant-a\", setID=\"set-id\"): aggregation handler failed: combine failed"
        ]
    );
}

#[tokio::test]
async fn run_once_ignores_delete_set_error_after_aggregated_enqueue() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let aggregator_logger: Arc<dyn Logger> = logger.clone();
    let broker = RecordingAggregationBroker {
        ready_set_id: Some("set-id".to_owned()),
        delete_error: Some(AggregationError::Broker(BrokerError::Other(
            "redis down".to_owned(),
        ))),
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

    assert_eq!(run.checked, 1);
    assert_eq!(run.aggregated, 1);
    let broker = &aggregator.broker;
    assert_eq!(broker.enqueued.len(), 1);
    assert_eq!(
        broker.deletes,
        [(
            "critical".to_owned(),
            "tenant-a".to_owned(),
            "set-id".to_owned()
        )]
    );
    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        [
            "Failed to delete aggregation set: queue=\"critical\", group=\"tenant-a\", setID=\"set-id\""
        ]
    );
}
