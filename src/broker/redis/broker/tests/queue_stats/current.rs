use super::*;

#[tokio::test]
async fn async_broker_reads_current_queue_stats() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_100);
    let executor = FakeExecutor {
        script_value_results: vec![
            redis::Value::Array(vec![
                redis_bulk("asynq:{critical}:pending"),
                redis::Value::Int(2),
                redis_bulk("asynq:{critical}:active"),
                redis::Value::Int(1),
                redis_bulk("asynq:{critical}:scheduled"),
                redis::Value::Int(3),
                redis_bulk("asynq:{critical}:retry"),
                redis::Value::Int(4),
                redis_bulk("asynq:{critical}:archived"),
                redis::Value::Int(5),
                redis_bulk("asynq:{critical}:completed"),
                redis::Value::Int(6),
                redis_bulk("asynq:{critical}:processed:2023-11-14"),
                redis::Value::Int(7),
                redis_bulk("asynq:{critical}:failed:2023-11-14"),
                redis::Value::Int(8),
                redis_bulk("asynq:{critical}:processed"),
                redis::Value::Int(9),
                redis_bulk("asynq:{critical}:failed"),
                redis::Value::Int(10),
                redis_bulk("asynq:{critical}:paused"),
                redis::Value::Int(1),
                redis_bulk("oldest_pending_since"),
                redis::Value::Int(1_700_000_000_000_000_000),
                redis_bulk("group_size"),
                redis::Value::Int(2),
                redis_bulk("aggregating_count"),
                redis::Value::Int(11),
            ]),
            redis::Value::Int(1234),
        ],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let stats = broker.current_queue_stats("critical").await.unwrap();

    assert_eq!(stats.queue(), "critical");
    assert_eq!(stats.memory_usage(), 1234);
    assert!(stats.paused());
    assert_eq!(stats.pending(), 2);
    assert_eq!(stats.active(), 1);
    assert_eq!(stats.scheduled(), 3);
    assert_eq!(stats.retry(), 4);
    assert_eq!(stats.archived(), 5);
    assert_eq!(stats.completed(), 6);
    assert_eq!(stats.processed(), 7);
    assert_eq!(stats.failed(), 8);
    assert_eq!(stats.processed_total(), 9);
    assert_eq!(stats.failed_total(), 10);
    assert_eq!(stats.aggregating(), 11);
    assert_eq!(stats.groups(), 2);
    assert_eq!(stats.size(), 32);
    assert_eq!(stats.latency(), Duration::from_secs(100));
    assert_eq!(stats.timestamp(), now);

    assert!(matches!(
        &broker.executor().calls[0],
        ExecutorCall::Sismember { key, member }
            if key == "asynq:queues" && member == "critical"
    ));
    assert!(matches!(
        &broker.executor().calls[1],
        ExecutorCall::EvalScriptValue { script, keys, args }
            if *script == RedisScript::CurrentQueueStats
                && keys[0] == "asynq:{critical}:pending"
                && args[0] == RedisArg::String("asynq:{critical}:t:".to_owned())
    ));
    assert!(matches!(
        &broker.executor().calls[2],
        ExecutorCall::EvalScriptValue { script, keys, args }
            if *script == RedisScript::QueueMemoryUsage
                && keys[0] == "asynq:{critical}:active"
                && args[1] == RedisArg::I64(20)
    ));
}

#[tokio::test]
async fn async_broker_current_queue_stats_reports_missing_queue() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        sismember_results: vec![false],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker.current_queue_stats("critical").await.unwrap_err();

    assert_eq!(error, AdminError::QueueNotFound);
    assert_eq!(broker.executor().calls.len(), 1);
}

#[tokio::test]
async fn async_broker_current_queue_stats_reads_clock_after_queue_check() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let call_log = Arc::new(Mutex::new(Vec::new()));
    let executor = FakeExecutor {
        call_log: Some(Arc::clone(&call_log)),
        script_value_results: vec![redis::Value::Array(Vec::new()), redis::Value::Int(0)],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(
        executor,
        RecordingClock {
            now,
            call_log: Arc::clone(&call_log),
        },
    );

    let stats = broker.current_queue_stats("critical").await.unwrap();

    assert_eq!(stats.timestamp(), now);
    assert_eq!(&*call_log.lock().unwrap(), &["sismember", "clock"]);
    let ExecutorCall::EvalScriptValue { args, .. } = &broker.executor().calls[1] else {
        panic!("expected current stats script call");
    };
    assert_eq!(
        args,
        &[
            RedisArg::String("asynq:{critical}:t:".to_owned()),
            RedisArg::String("asynq:{critical}:g:".to_owned()),
        ]
    );
}

#[tokio::test]
async fn async_broker_current_queue_stats_uses_second_clock_for_latency() {
    let stats_now = UNIX_EPOCH + Duration::from_secs(1_700_000_100);
    let latency_now = UNIX_EPOCH + Duration::from_secs(1_700_000_125);
    let call_log = Arc::new(Mutex::new(Vec::new()));
    let executor = FakeExecutor {
        call_log: Some(Arc::clone(&call_log)),
        script_value_results: vec![
            redis::Value::Array(vec![
                redis_bulk("oldest_pending_since"),
                redis::Value::Int(1_700_000_000_000_000_000),
            ]),
            redis::Value::Int(0),
        ],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(
        executor,
        SequenceRecordingClock {
            times: Arc::new(Mutex::new(vec![latency_now, stats_now])),
            call_log: Arc::clone(&call_log),
        },
    );

    let stats = broker.current_queue_stats("critical").await.unwrap();

    assert_eq!(stats.timestamp(), stats_now);
    assert_eq!(stats.latency(), Duration::from_secs(125));
    assert_eq!(stats.latency_nanos(), 125_000_000_000);
    assert_eq!(&*call_log.lock().unwrap(), &["sismember", "clock", "clock"]);
}

#[tokio::test]
async fn async_broker_current_queue_stats_preserves_negative_latency() {
    let stats_now = UNIX_EPOCH + Duration::from_secs(1_700_000_100);
    let latency_now = UNIX_EPOCH + Duration::from_secs(1_700_000_125);
    let executor = FakeExecutor {
        script_value_results: vec![
            redis::Value::Array(vec![
                redis_bulk("oldest_pending_since"),
                redis::Value::Int(1_700_000_150_000_000_000),
            ]),
            redis::Value::Int(0),
        ],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(
        executor,
        SequenceRecordingClock {
            times: Arc::new(Mutex::new(vec![latency_now, stats_now])),
            call_log: Arc::new(Mutex::new(Vec::new())),
        },
    );

    let stats = broker.current_queue_stats("critical").await.unwrap();

    assert_eq!(stats.latency(), Duration::ZERO);
    assert_eq!(stats.latency_nanos(), -25_000_000_000);
}
