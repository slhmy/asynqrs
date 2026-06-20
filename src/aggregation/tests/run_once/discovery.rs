use super::*;

#[tokio::test]
async fn run_once_discovers_groups_from_broker() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let broker = RecordingAggregationBroker {
        ready_set_id: Some("set-id".to_owned()),
        groups: vec!["tenant-a".to_owned(), "tenant-b".to_owned()],
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

    let summary = run_aggregator_once(&mut aggregator).await.unwrap();

    assert_eq!(summary.checked, 2);
    assert_eq!(summary.aggregated, 2);
    assert_eq!(summary.reclaimed, 1);
    let broker = &aggregator.broker;
    assert_eq!(broker.group_lists, ["critical"]);
    assert_eq!(
        broker.checks,
        [
            (
                "critical".to_owned(),
                "tenant-a".to_owned(),
                now,
                Duration::from_secs(10),
                Duration::from_secs(60),
                2,
            ),
            (
                "critical".to_owned(),
                "tenant-b".to_owned(),
                now,
                Duration::from_secs(10),
                Duration::from_secs(60),
                2,
            )
        ]
    );
}
