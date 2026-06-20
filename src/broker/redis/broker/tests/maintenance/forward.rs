use super::*;

#[tokio::test]
async fn async_broker_forwards_scheduled_tasks_with_forward_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![2, 0],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let moved = broker
        .forward_with_now("critical", now, true)
        .await
        .unwrap();

    assert_eq!(moved, 2);
    let calls = &broker.executor().calls;
    assert_eq!(calls.len(), 2);
    let ExecutorCall::EvalScriptInt { script, keys, args } = &calls[0] else {
        panic!("expected forward script call");
    };
    assert_eq!(*script, RedisScript::Forward);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:scheduled".to_owned(),
            "asynq:{critical}:pending".to_owned(),
        ]
    );
    assert_eq!(args[0], RedisArg::I64(1_700_000_000));
    assert_eq!(args[1], RedisArg::String("asynq:{critical}:t:".to_owned()));
    assert_eq!(args[2], RedisArg::I64(1_700_000_000_000_000_000));
    assert_eq!(args[3], RedisArg::String("asynq:{critical}:g:".to_owned()));
}

#[tokio::test]
async fn async_broker_forward_allows_blank_queue_name() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![0],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let moved = broker.forward_with_now(" ", now, true).await.unwrap();

    assert_eq!(moved, 0);
    assert_eq!(broker.executor().calls.len(), 1);
    let ExecutorCall::EvalScriptInt { script, keys, args } = &broker.executor().calls[0] else {
        panic!("expected forward script call");
    };
    assert_eq!(*script, RedisScript::Forward);
    assert_eq!(
        keys,
        &[
            "asynq:{ }:scheduled".to_owned(),
            "asynq:{ }:pending".to_owned()
        ]
    );
    assert_eq!(
        args,
        &[
            RedisArg::I64(1_700_000_000),
            RedisArg::String("asynq:{ }:t:".to_owned()),
            RedisArg::I64(1_700_000_000_000_000_000),
            RedisArg::String("asynq:{ }:g:".to_owned()),
        ]
    );
}

#[tokio::test]
async fn async_broker_forwards_retry_tasks_with_forward_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![1, 0],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let moved = broker
        .forward_with_now("critical", now, false)
        .await
        .unwrap();

    assert_eq!(moved, 1);
    let ExecutorCall::EvalScriptInt { script, keys, .. } = &broker.executor().calls[0] else {
        panic!("expected forward script call");
    };
    assert_eq!(*script, RedisScript::Forward);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:retry".to_owned(),
            "asynq:{critical}:pending".to_owned(),
        ]
    );
}

#[tokio::test]
async fn async_broker_forwards_until_delayed_set_is_empty() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![100, 5, 0],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let moved = broker
        .forward_with_now("critical", now, true)
        .await
        .unwrap();

    assert_eq!(moved, 105);
    assert_eq!(broker.executor().calls.len(), 3);
}

#[tokio::test]
async fn forwards_scheduled_tasks_with_forward_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![2, 0],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let moved = broker.forward_scheduled("critical").await.unwrap();

    assert_eq!(moved, 2);
    let calls = &broker.executor().calls;
    assert_eq!(calls.len(), 2);
    let ExecutorCall::EvalScriptInt { script, keys, args } = &calls[0] else {
        panic!("expected forward script call");
    };
    assert_eq!(*script, RedisScript::Forward);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:scheduled".to_owned(),
            "asynq:{critical}:pending".to_owned(),
        ]
    );
    assert_eq!(args[0], RedisArg::I64(1_700_000_000));
    assert_eq!(args[1], RedisArg::String("asynq:{critical}:t:".to_owned()));
    assert_eq!(args[2], RedisArg::I64(1_700_000_000_000_000_000));
    assert_eq!(args[3], RedisArg::String("asynq:{critical}:g:".to_owned()));
}

#[tokio::test]
async fn forwards_retry_tasks_with_forward_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![1, 0],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let moved = broker.forward_retry("critical").await.unwrap();

    assert_eq!(moved, 1);
    let ExecutorCall::EvalScriptInt { script, keys, .. } = &broker.executor().calls[0] else {
        panic!("expected forward script call");
    };
    assert_eq!(*script, RedisScript::Forward);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:retry".to_owned(),
            "asynq:{critical}:pending".to_owned(),
        ]
    );
}

#[tokio::test]
async fn forwards_until_delayed_set_is_empty() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![100, 5, 0],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let moved = broker.forward_scheduled("critical").await.unwrap();

    assert_eq!(moved, 105);
    assert_eq!(broker.executor().calls.len(), 3);
}

#[tokio::test]
async fn forward_if_ready_iterates_queues_and_delayed_sets_in_broker() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_int_results: vec![2, 0, 1, 0, 3, 0, 4, 0],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let result = broker
        .forward_if_ready_with_now(&["critical".to_owned(), "default".to_owned()], now)
        .await
        .unwrap();

    assert_eq!(result.forwarded_scheduled(), 5);
    assert_eq!(result.forwarded_retry(), 5);
    let delayed_keys: Vec<String> = broker
        .executor()
        .calls
        .iter()
        .map(|call| match call {
            ExecutorCall::EvalScriptInt { keys, .. } => keys[0].clone(),
            _ => panic!("expected forward script call"),
        })
        .collect();
    assert_eq!(
        delayed_keys,
        [
            "asynq:{critical}:scheduled".to_owned(),
            "asynq:{critical}:scheduled".to_owned(),
            "asynq:{critical}:retry".to_owned(),
            "asynq:{critical}:retry".to_owned(),
            "asynq:{default}:scheduled".to_owned(),
            "asynq:{default}:scheduled".to_owned(),
            "asynq:{default}:retry".to_owned(),
            "asynq:{default}:retry".to_owned(),
        ]
    );
}
