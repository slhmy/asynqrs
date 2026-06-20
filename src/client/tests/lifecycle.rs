use super::*;

#[test]
fn ping_delegates_to_broker() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        FixedTaskIdGenerator("task-id"),
        FixedClock(now),
    );

    client.ping().unwrap();

    assert_eq!(client.broker.ping_calls, 1);
}

#[test]
fn ping_rust_method_matches_upstream_ping_behavior() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        FixedTaskIdGenerator("task-id"),
        FixedClock(now),
    );

    client.ping().unwrap();

    assert_eq!(client.broker.ping_calls, 1);
}

#[test]
fn ping_errors_are_propagated() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let broker = RecordingBroker {
        ping_error: Some(BrokerError::Other("redis down".to_owned())),
        ..RecordingBroker::default()
    };
    let mut client = Client::with_parts(broker, FixedTaskIdGenerator("task-id"), FixedClock(now));

    let error = client.ping().unwrap_err();

    assert_eq!(
        error,
        ClientError::Ping(BrokerError::Other("redis down".to_owned()))
    );
    assert_eq!(error.to_string(), "redis down");
    assert_eq!(client.broker.ping_calls, 1);
}

#[test]
fn close_delegates_to_broker() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        FixedTaskIdGenerator("task-id"),
        FixedClock(now),
    );

    client.close().unwrap();

    assert_eq!(client.broker.close_calls, 1);
}

#[test]
fn close_rust_method_matches_upstream_close_behavior() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        FixedTaskIdGenerator("task-id"),
        FixedClock(now),
    );

    client.close().unwrap();

    assert_eq!(client.broker.close_calls, 1);
}

#[test]
fn close_errors_are_propagated() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let broker = RecordingBroker {
        close_error: Some(BrokerError::Other("redis down".to_owned())),
        ..RecordingBroker::default()
    };
    let mut client = Client::with_parts(broker, FixedTaskIdGenerator("task-id"), FixedClock(now));

    let error = client.close().unwrap_err();

    assert_eq!(
        error,
        ClientError::Close(BrokerError::Other("redis down".to_owned()))
    );
    assert_eq!(error.to_string(), "redis down");
    assert_eq!(client.broker.close_calls, 1);
}

#[test]
fn close_refuses_shared_connections_like_upstream() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut client = Client::with_shared_connection(
        RecordingBroker::default(),
        FixedTaskIdGenerator("task-id"),
        FixedClock(now),
    );

    let error = client.close().unwrap_err();

    assert!(client.shared_connection);
    assert_eq!(error, ClientError::SharedConnection);
    assert_eq!(
        error.to_string(),
        "redis connection is shared so the Client can't be closed through asynq"
    );
    assert_eq!(client.broker.close_calls, 0);
}
