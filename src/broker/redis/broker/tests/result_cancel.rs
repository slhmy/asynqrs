use super::*;

#[tokio::test]
async fn async_broker_writes_task_result() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    let written = broker
        .write_result("critical", "task-id", b"result".to_vec())
        .await
        .unwrap();

    assert_eq!(written, 6);
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::HsetBytes {
            key: "asynq:{critical}:t:task-id".to_owned(),
            field: "result".to_owned(),
            value: b"result".to_vec(),
        }]
    );
}

#[tokio::test]
async fn worker_broker_core_writes_task_result_without_dyn_broker_boundary() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    let written = crate::server::WorkerBrokerCore::write_result(
        &mut broker,
        "critical",
        "task-id",
        b"result".to_vec(),
    )
    .await
    .unwrap();

    assert_eq!(written, 6);
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::HsetBytes {
            key: "asynq:{critical}:t:task-id".to_owned(),
            field: "result".to_owned(),
            value: b"result".to_vec(),
        }]
    );
}

#[tokio::test]
async fn async_broker_write_result_allows_blank_queue_name() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    let written = broker
        .write_result(" ", "task-id", b"result".to_vec())
        .await
        .unwrap();

    assert_eq!(written, 6);
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::HsetBytes {
            key: "asynq:{ }:t:task-id".to_owned(),
            field: "result".to_owned(),
            value: b"result".to_vec(),
        }]
    );
}

#[tokio::test]
async fn async_broker_write_result_maps_executor_errors() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        hset_error: Some(RedisExecutorError::new("connection closed")),
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker
        .write_result("critical", "task-id", b"result".to_vec())
        .await
        .unwrap_err();

    assert_eq!(error, ResultError::Other("connection closed".to_owned()));
}

#[tokio::test]
async fn async_broker_publishes_cancel_task_id() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    let subscribers = broker.publish_cancel("task-id").await.unwrap();

    assert_eq!(subscribers, 1);
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::Publish {
            channel: "asynq:cancel".to_owned(),
            payload: "task-id".to_owned(),
        }]
    );
}

#[tokio::test]
async fn async_broker_cancel_processing_publishes_cancel_task_id() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker.cancel_processing("task-id").await.unwrap();

    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::Publish {
            channel: "asynq:cancel".to_owned(),
            payload: "task-id".to_owned(),
        }]
    );
}

#[tokio::test]
async fn async_broker_publish_cancel_allows_empty_task_id() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    let subscribers = broker.publish_cancel(" ").await.unwrap();

    assert_eq!(subscribers, 1);
    assert_eq!(
        broker.executor().calls,
        [ExecutorCall::Publish {
            channel: "asynq:cancel".to_owned(),
            payload: " ".to_owned(),
        }]
    );
}

#[tokio::test]
async fn async_broker_publish_cancel_maps_executor_errors() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        publish_error: Some(RedisExecutorError::new("connection closed")),
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker.publish_cancel("task-id").await.unwrap_err();

    assert_eq!(error, CancelError::Other("connection closed".to_owned()));
}
