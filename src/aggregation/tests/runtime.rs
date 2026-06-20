use super::*;

#[tokio::test]
async fn run_until_stopped_exits_on_shutdown() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let aggregator = Aggregator::with_clock(
        RecordingAggregationBroker::default(),
        RecordingAggregationHandler::default(),
        TestClock(now),
    );
    let mut aggregator = aggregator.with_test_tick_interval(Duration::from_millis(1));
    let mut sleeper = TokioSleeper;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();

    let summary = aggregator
        .run_until_stopped(&mut sleeper, shutdown_rx)
        .await
        .unwrap();

    assert_eq!(summary.checked, 0);
}

#[tokio::test]
async fn run_until_stopped_logs_shutdown_wait_debug_messages() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let aggregator_logger: Arc<dyn Logger> = logger.clone();
    let aggregator = Aggregator::with_clock(
        RecordingAggregationBroker::default(),
        RecordingAggregationHandler::default(),
        TestClock(now),
    )
    .with_test_logger(aggregator_logger)
    .with_log_level(LogLevel::Debug);
    let mut aggregator = aggregator.with_test_tick_interval(Duration::from_millis(1));
    let mut sleeper = TokioSleeper;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();

    let summary = aggregator
        .run_until_stopped(&mut sleeper, shutdown_rx)
        .await
        .unwrap();

    assert_eq!(summary.checked, 0);
    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        [
            "Waiting for all aggregation checks to finish...",
            "Aggregator done",
        ]
    );
}

#[tokio::test]
async fn run_until_stopped_waits_for_first_tick_before_checking_groups() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut aggregator = Aggregator::with_clock(
        RecordingAggregationBroker::default(),
        RecordingAggregationHandler::default(),
        TestClock(now),
    )
    .with_test_tick_interval(Duration::from_millis(1));
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
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut sleeper = ShutdownOnFirstSleep {
        shutdown: shutdown_tx,
    };

    let summary = aggregator
        .run_until_stopped(&mut sleeper, shutdown_rx)
        .await
        .unwrap();

    assert_eq!(summary.checked, 0);
}

#[tokio::test]
async fn run_until_stopped_skips_ticks_when_aggregation_checks_are_full() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let checks_started = Arc::new(AtomicUsize::new(0));
    let (release_tx, release_rx) = watch::channel(false);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut aggregator = Aggregator::with_clock(
        BlockingAggregationBroker {
            checks_started: Arc::clone(&checks_started),
            release: release_rx,
        },
        RecordingAggregationHandler::default(),
        TestClock(now),
    )
    .with_test_tick_interval(Duration::from_millis(1));
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
    let mut sleeper = ShutdownAfterSleeps {
        sleeps: 0,
        release: release_tx,
        shutdown: shutdown_tx,
    };

    let summary = aggregator
        .run_until_stopped(&mut sleeper, shutdown_rx)
        .await
        .unwrap();

    assert_eq!(
        checks_started.load(Ordering::SeqCst),
        MAX_CONCURRENT_AGGREGATION_CHECKS
    );
    assert_eq!(
        MAX_CONCURRENT_AGGREGATION_CHECKS,
        MAX_CONCURRENT_AGGREGATION_CHECKS
    );
    assert_eq!(summary.checked, MAX_CONCURRENT_AGGREGATION_CHECKS);
    assert_eq!(summary.skipped, 1);
}
