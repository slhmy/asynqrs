use super::*;

#[tokio::test]
async fn async_broker_reads_historical_queue_stats() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_value_results: vec![redis::Value::Array(vec![
            redis::Value::Int(4),
            redis::Value::Int(1),
            redis::Value::Int(9),
            redis::Value::Int(2),
        ])],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let stats = broker
        .historical_queue_stats_with_now("critical", now, 2)
        .await
        .unwrap();

    assert_eq!(stats.len(), 2);
    assert_eq!(stats[0].queue(), "critical");
    assert_eq!(stats[0].processed(), 4);
    assert_eq!(stats[0].failed(), 1);
    assert_eq!(stats[0].date(), now);
    assert_eq!(stats[0].time(), now);
    assert_eq!(stats[1].processed(), 9);
    assert_eq!(stats[1].failed(), 2);
    assert_eq!(stats[1].time(), now - Duration::from_secs(24 * 60 * 60));
    assert!(matches!(
        &broker.executor().calls[0],
        ExecutorCall::Sismember { key, member }
            if key == "asynq:queues" && member == "critical"
    ));
    assert!(matches!(
        &broker.executor().calls[1],
        ExecutorCall::EvalScriptValue { script, keys, args }
            if *script == RedisScript::HistoricalQueueStats
                && keys == &[
                    "asynq:{critical}:processed:2023-11-14".to_owned(),
                    "asynq:{critical}:failed:2023-11-14".to_owned(),
                    "asynq:{critical}:processed:2023-11-13".to_owned(),
                    "asynq:{critical}:failed:2023-11-13".to_owned(),
                ]
                && args.is_empty()
    ));
}

#[tokio::test]
async fn async_broker_historical_queue_stats_reads_clock_after_queue_check() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let call_log = Arc::new(Mutex::new(Vec::new()));
    let executor = FakeExecutor {
        call_log: Some(Arc::clone(&call_log)),
        script_value_results: vec![redis::Value::Array(vec![
            redis::Value::Int(0),
            redis::Value::Int(0),
        ])],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(
        executor,
        RecordingClock {
            now,
            call_log: Arc::clone(&call_log),
        },
    );

    let stats = broker.historical_queue_stats("critical", 1).await.unwrap();

    assert_eq!(stats[0].time(), now);
    assert_eq!(&*call_log.lock().unwrap(), &["sismember", "clock"]);
    let ExecutorCall::EvalScriptValue { keys, .. } = &broker.executor().calls[1] else {
        panic!("expected historical stats script call");
    };
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:processed:2023-11-14".to_owned(),
            "asynq:{critical}:failed:2023-11-14".to_owned(),
        ]
    );
}

#[tokio::test]
async fn async_broker_historical_queue_stats_rejects_zero_days() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    let error = broker
        .historical_queue_stats_with_now("critical", now, 0)
        .await
        .unwrap_err();

    assert_eq!(error, AdminError::NonPositiveDays);
    assert!(broker.executor().calls.is_empty());
}

#[tokio::test]
async fn async_broker_historical_queue_stats_rejects_zero_days_before_clock() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let call_log = Arc::new(Mutex::new(Vec::new()));
    let mut broker = RedisBroker::with_clock(
        FakeExecutor::default(),
        RecordingClock {
            now,
            call_log: Arc::clone(&call_log),
        },
    );

    let error = broker
        .historical_queue_stats("critical", 0)
        .await
        .unwrap_err();

    assert_eq!(error, AdminError::NonPositiveDays);
    assert!(broker.executor().calls.is_empty());
    assert!(call_log.lock().unwrap().is_empty());
}

#[tokio::test]
async fn async_broker_historical_queue_stats_rejects_negative_days_before_clock() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let call_log = Arc::new(Mutex::new(Vec::new()));
    let mut broker = RedisBroker::with_clock(
        FakeExecutor::default(),
        RecordingClock {
            now,
            call_log: Arc::clone(&call_log),
        },
    );

    let error = broker
        .historical_queue_stats("critical", -1)
        .await
        .unwrap_err();

    assert_eq!(error, AdminError::NonPositiveDays);
    assert!(broker.executor().calls.is_empty());
    assert!(call_log.lock().unwrap().is_empty());
}
