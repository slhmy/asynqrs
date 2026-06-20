use super::*;

#[test]
fn enqueue_scoped_rust_method_matches_upstream_scope_and_option_path() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        FixedTaskIdGenerator("generated-id"),
        FixedClock(now),
    );
    let scope = ClientEnqueueScope::background();

    let result = client
        .enqueue_scoped_with(
            &scope,
            &task,
            EnqueueOptions::new()
                .queue(crate::QueueName::new("critical").unwrap())
                .task_id(crate::TaskId::new("enqueue-scope-id").unwrap()),
        )
        .unwrap();

    assert_eq!(result.id(), "enqueue-scope-id");
    assert_eq!(result.queue(), "critical");
    assert_eq!(client.broker.plans[0].message().id, "enqueue-scope-id");
    assert_eq!(client.broker.plans[0].message().queue, "critical");
}

#[test]
fn enqueue_scoped_optional_rejects_nil_task_like_upstream() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        FixedTaskIdGenerator("task-id"),
        FixedClock(now),
    );
    let scope = ClientEnqueueScope::background();

    let error = client
        .enqueue_scoped_optional_with(
            &scope,
            None,
            EnqueueOptions::new().queue(crate::QueueName::new("critical").unwrap()),
        )
        .unwrap_err();

    assert_eq!(error, ClientError::NilTask);
    assert_eq!(error.to_string(), "task cannot be nil");
    assert!(client.broker.plans.is_empty());
}

#[test]
fn enqueue_scoped_cancelled_scope_stops_before_broker_enqueue_like_upstream() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        FixedTaskIdGenerator("task-id"),
        FixedClock(now),
    );
    let scope = ClientEnqueueScope::cancelled();

    let error = client
        .enqueue_scoped_with(
            &scope,
            &task,
            EnqueueOptions::new().queue(crate::QueueName::new("critical").unwrap()),
        )
        .unwrap_err();

    assert_eq!(error, ClientError::Cancelled);
    assert_eq!(error.to_string(), "context canceled");
    assert!(client.broker.plans.is_empty());
}

#[test]
fn enqueue_scoped_observes_live_cancellation_token() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        FixedTaskIdGenerator("task-id"),
        FixedClock(now),
    );
    let cancellation = tokio_util::sync::CancellationToken::new();
    let scope = ClientEnqueueScope::from_cancellation_token(cancellation.clone());
    cancellation.cancel();

    let error = client
        .enqueue_scoped_with(
            &scope,
            &task,
            EnqueueOptions::new().queue(crate::QueueName::new("critical").unwrap()),
        )
        .unwrap_err();

    assert_eq!(error, ClientError::Cancelled);
    assert!(client.broker.plans.is_empty());
}

#[test]
fn enqueue_scoped_checks_cancellation_after_task_and_option_validation_like_upstream() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());
    let calls = Rc::new(Cell::new(0));
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        CountingTaskIdGenerator {
            calls: Rc::clone(&calls),
        },
        FixedClock(now),
    );
    let scope = ClientEnqueueScope::cancelled();

    let error = client
        .enqueue_scoped_with(
            &scope,
            &task,
            EnqueueOptions::new().unique_for(Duration::ZERO),
        )
        .unwrap_err();

    assert!(matches!(error, ClientError::Plan(_)));
    assert_eq!(error.to_string(), "Unique TTL cannot be less than 1s");
    assert_eq!(calls.get(), 1);
    assert!(client.broker.plans.is_empty());
}
