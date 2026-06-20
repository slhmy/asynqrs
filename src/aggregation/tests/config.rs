use super::*;

#[test]
fn aggregation_tick_interval_defaults_to_upstream_interval() {
    let aggregator = Aggregator::with_clock(
        RecordingAggregationBroker::default(),
        RecordingAggregationHandler::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    );

    assert_eq!(aggregator.tick_interval, Duration::from_secs(7));
    assert_eq!(
        DEFAULT_AGGREGATOR_TICK_INTERVAL,
        DEFAULT_AGGREGATOR_TICK_INTERVAL
    );
}

#[test]
fn aggregation_tick_interval_uses_shorter_grace_period() {
    let mut aggregator = Aggregator::with_clock(
        RecordingAggregationBroker::default(),
        RecordingAggregationHandler::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    );

    aggregator.add_test_group(
        AggregationGroup::new(
            "critical",
            "tenant-a",
            Duration::from_secs(2),
            Duration::from_secs(60),
            2,
        )
        .unwrap(),
    );
    aggregator.add_auto_group_config(
        AggregationGroupConfig::new(
            "default",
            Duration::from_secs(1),
            Duration::from_secs(60),
            2,
        )
        .unwrap(),
    );

    assert_eq!(aggregator.tick_interval, Duration::from_secs(1));
}

#[test]
fn explicit_aggregation_tick_interval_can_be_shorter_than_grace_periods() {
    let mut aggregator = Aggregator::with_clock(
        RecordingAggregationBroker::default(),
        RecordingAggregationHandler::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .with_test_tick_interval(Duration::from_millis(500));

    aggregator.add_test_group(
        AggregationGroup::new(
            "critical",
            "tenant-a",
            Duration::from_secs(2),
            Duration::from_secs(60),
            2,
        )
        .unwrap(),
    );

    assert_eq!(aggregator.tick_interval, Duration::from_millis(500));
}

#[test]
fn aggregation_group_allows_zero_max_size_as_unlimited() {
    let group = AggregationGroup::new(
        "critical",
        "tenant-a",
        Duration::from_secs(2),
        Duration::from_secs(60),
        0,
    )
    .unwrap();
    let config =
        AggregationGroupConfig::new("critical", Duration::from_secs(2), Duration::ZERO, 0).unwrap();

    assert_eq!(group.max_size(), 0);
    assert_eq!(config.max_size(), 0);
}
