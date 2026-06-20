use super::*;

#[test]
fn plan_errors_happen_before_broker_enqueue() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new(" ", Vec::new());
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        FixedTaskIdGenerator("task-id"),
        FixedClock(now),
    );

    let error = client.enqueue(&task).unwrap_err();

    assert_eq!(error, ClientError::Plan(EnqueuePlanError::EmptyTaskType));
    assert_eq!(error.to_string(), "task typename cannot be empty");
    assert!(client.broker.plans.is_empty());
}

#[test]
fn task_type_validation_happens_before_task_id_generation() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new(" ", Vec::new());
    let calls = Rc::new(Cell::new(0));
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        CountingTaskIdGenerator {
            calls: Rc::clone(&calls),
        },
        FixedClock(now),
    );

    let error = client.enqueue(&task).unwrap_err();

    assert_eq!(error, ClientError::Plan(EnqueuePlanError::EmptyTaskType));
    assert_eq!(calls.get(), 0);
    assert!(client.broker.plans.is_empty());
}

#[test]
fn broker_errors_are_propagated() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());
    let broker = RecordingBroker {
        error: Some(BrokerError::TaskIdConflict),
        ..RecordingBroker::default()
    };
    let mut client = Client::with_parts(broker, FixedTaskIdGenerator("task-id"), FixedClock(now));

    let error = client.enqueue(&task).unwrap_err();

    assert_eq!(error, ClientError::Broker(BrokerError::TaskIdConflict));
    assert_eq!(error.to_string(), "task ID conflicts with another task");
    assert!(client.broker.plans.is_empty());
}

#[test]
fn broker_error_operation_variant_exposes_structured_context() {
    let error = BrokerError::operation("enqueue", "redis down");

    assert_eq!(error.operation_name(), Some("enqueue"));
    assert_eq!(error.message(), Some("redis down"));
    assert_eq!(error.to_string(), "enqueue failed: redis down");
}
