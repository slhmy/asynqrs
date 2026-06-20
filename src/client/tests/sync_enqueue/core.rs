use super::*;

#[test]
fn enqueue_records_plan_and_returns_result() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let deadline = now + Duration::from_secs(120);
    let task = Task::with_headers("email:welcome", b"payload".to_vec(), [("trace-id", "abc")]);
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        FixedTaskIdGenerator("task-id"),
        FixedClock(now),
    );

    let result = client
        .enqueue_with(
            &task,
            EnqueueOptions::new()
                .queue(crate::QueueName::new("critical").unwrap())
                .max_retries(7)
                .timeout(Duration::from_secs(45))
                .deadline(deadline)
                .retain_for(Duration::from_secs(300)),
        )
        .unwrap();

    assert_eq!(result.id(), "task-id");
    assert_eq!(result.queue(), "critical");
    assert_eq!(result.type_name(), "email:welcome");
    assert_eq!(result.payload(), b"payload");
    assert_eq!(
        result.headers().get("trace-id").map(String::as_str),
        Some("abc")
    );
    assert_eq!(result.max_retry(), 7);
    assert_eq!(result.retried(), 0);
    assert_eq!(result.last_error(), "");
    assert_eq!(result.last_failed_at(), None);
    assert_eq!(result.timeout(), Duration::from_secs(45));
    assert_eq!(result.deadline(), Some(deadline));
    assert_eq!(result.group(), "");
    assert!(!result.is_orphaned());
    assert_eq!(result.retention(), Duration::from_secs(300));
    assert_eq!(result.completed_at(), None);
    assert!(result.result().is_empty());
    assert_eq!(result.state(), TaskState::Pending);
    assert_eq!(result.next_process_at(), Some(now));

    let plans = &client.broker.plans;
    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].message().id, "task-id");
    assert_eq!(plans[0].message().queue, "critical");
    assert_eq!(plans[0].state(), TaskState::Pending);
}

#[test]
fn enqueue_rust_method_matches_upstream_enqueue_behavior() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", b"payload".to_vec());
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        FixedTaskIdGenerator("task-id"),
        FixedClock(now),
    );

    let result = client.enqueue(&task).unwrap();

    assert_eq!(result.id(), "task-id");
    assert_eq!(result.type_name(), "email:welcome");
    assert_eq!(client.broker.plans.len(), 1);
}

#[test]
fn enqueue_optional_rejects_nil_task_like_upstream_enqueue() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        FixedTaskIdGenerator("task-id"),
        FixedClock(now),
    );

    let error = client.enqueue_optional(None).unwrap_err();

    assert_eq!(error, ClientError::NilTask);
    assert_eq!(error.to_string(), "task cannot be nil");
    assert!(client.broker.plans.is_empty());
}

#[test]
fn enqueue_result_exposes_group() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        FixedTaskIdGenerator("task-id"),
        FixedClock(now),
    );

    let result = client
        .enqueue_with(
            &task,
            EnqueueOptions::new().group(crate::GroupName::new("tenant-a").unwrap()),
        )
        .unwrap();

    assert_eq!(result.group(), "tenant-a");
    assert_eq!(result.state(), TaskState::Aggregating);
    assert_eq!(result.next_process_at(), None);
    assert_eq!(client.broker.plans[0].message().group_key, "tenant-a");
}

#[test]
fn enqueue_with_applies_call_options() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        FixedTaskIdGenerator("generated-id"),
        FixedClock(now),
    );

    let result = client
        .enqueue_with(
            &task,
            EnqueueOptions::new()
                .queue(crate::QueueName::new("critical").unwrap())
                .task_id(crate::TaskId::new("enqueue-option-id").unwrap()),
        )
        .unwrap();

    assert_eq!(result.id(), "enqueue-option-id");
    assert_eq!(result.queue(), "critical");
    assert_eq!(client.broker.plans[0].message().id, "enqueue-option-id");
}

#[test]
fn enqueue_with_uses_rust_native_options() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        FixedTaskIdGenerator("task-id"),
        FixedClock(now),
    );

    let result = client
        .enqueue_with(
            &task,
            EnqueueOptions::new()
                .queue(crate::QueueName::new("critical").unwrap())
                .max_retries(5)
                .timeout(Duration::from_secs(30)),
        )
        .unwrap();

    assert_eq!(result.queue(), "critical");
    assert_eq!(result.max_retry(), 5);
    assert_eq!(result.timeout(), Duration::from_secs(30));
    assert_eq!(client.broker.plans[0].message().queue, "critical");
    assert_eq!(client.broker.plans[0].message().retry, 5);
    assert_eq!(client.broker.plans[0].message().timeout, 30);
}

#[test]
fn enqueue_optional_with_rejects_nil_task_before_planning_options() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        FixedTaskIdGenerator("task-id"),
        FixedClock(now),
    );

    let error = client
        .enqueue_optional_with(
            None,
            EnqueueOptions::new()
                .queue(crate::QueueName::new("critical").unwrap())
                .task_id(crate::TaskId::new("option-task-id").unwrap()),
        )
        .unwrap_err();

    assert_eq!(error, ClientError::NilTask);
    assert_eq!(error.to_string(), "task cannot be nil");
    assert!(client.broker.plans.is_empty());
}
