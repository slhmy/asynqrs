use super::*;

#[tokio::test]
async fn run_once_flushes_ready_aggregation_set() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let broker = RecordingAggregationBroker {
        ready_set_id: Some("set-id".to_owned()),
        ..RecordingAggregationBroker::default()
    };
    let mut aggregator = Aggregator::with_clock(
        broker,
        RecordingAggregationHandler::default(),
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

    let summary = run_aggregator_once(&mut aggregator).await.unwrap();

    assert_eq!(summary.checked, 1);
    assert_eq!(summary.aggregated, 1);
    assert_eq!(summary.reclaimed, 1);
    let broker = &aggregator.broker;
    assert_eq!(broker.checks.len(), 1);
    assert_eq!(
        broker.reads,
        [(
            "critical".to_owned(),
            "tenant-a".to_owned(),
            "set-id".to_owned()
        )]
    );
    assert_eq!(
        broker.deletes,
        [(
            "critical".to_owned(),
            "tenant-a".to_owned(),
            "set-id".to_owned()
        )]
    );
    assert_eq!(broker.enqueued.len(), 1);
    let enqueued = broker.enqueued[0].message();
    assert_eq!(enqueued.r#type, "email:batch");
    assert_eq!(enqueued.payload, b"aggregated");
    assert_eq!(enqueued.queue, "critical");
    assert!(enqueued.group_key.is_empty());
    let handler = Arc::clone(&aggregator.handler);
    let handler = handler.lock().await;
    assert_eq!(
        handler.handled,
        [(
            "critical".to_owned(),
            "tenant-a".to_owned(),
            "set-id".to_owned(),
            1
        )]
    );
}

#[tokio::test]
async fn run_once_passes_unlimited_max_size_to_broker() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut aggregator = Aggregator::with_clock(
        RecordingAggregationBroker::default(),
        RecordingAggregationHandler::default(),
        TestClock(now),
    );
    aggregator.add_test_group(
        AggregationGroup::new(
            "critical",
            "tenant-a",
            Duration::from_secs(10),
            Duration::from_secs(60),
            0,
        )
        .unwrap(),
    );

    let summary = run_aggregator_once(&mut aggregator).await.unwrap();

    assert_eq!(summary.checked, 1);
    let broker = &aggregator.broker;
    assert_eq!(
        broker.checks,
        [(
            "critical".to_owned(),
            "tenant-a".to_owned(),
            now,
            Duration::from_secs(10),
            Duration::from_secs(60),
            0,
        )]
    );
}

#[tokio::test]
async fn run_once_overrides_aggregated_task_queue_with_group_queue_like_upstream() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let broker = RecordingAggregationBroker {
        ready_set_id: Some("set-id".to_owned()),
        ..RecordingAggregationBroker::default()
    };
    let mut aggregator = Aggregator::with_clock(
        broker,
        RecordingAggregationHandler::default(),
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

    let summary = run_aggregator_once(&mut aggregator).await.unwrap();

    assert_eq!(summary.aggregated, 1);
    let broker = &aggregator.broker;
    assert_eq!(broker.enqueued.len(), 1);
    // Reference: Asynq v0.26.0 aggregator enqueues aggregated tasks with
    // `Queue(qname)`, so the group queue overrides any queue option returned
    // by the user aggregator:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/aggregator.go#L127>.
    assert_eq!(broker.enqueued[0].message().queue, "critical");
}

#[tokio::test]
async fn run_once_leaves_non_ready_group_alone() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut aggregator = Aggregator::with_clock(
        RecordingAggregationBroker::default(),
        RecordingAggregationHandler::default(),
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

    let summary = run_aggregator_once(&mut aggregator).await.unwrap();

    assert_eq!(summary.checked, 1);
    assert_eq!(summary.aggregated, 0);
    let broker = &aggregator.broker;
    assert!(broker.reads.is_empty());
    assert!(broker.enqueued.is_empty());
    assert!(broker.deletes.is_empty());
    let handler = Arc::clone(&aggregator.handler);
    let handler = handler.lock().await;
    assert!(handler.handled.is_empty());
}
