use super::*;

#[test]
fn plans_default_pending_enqueue() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", b"payload".to_vec());

    let plan = EnqueuePlan::from_task(&task, now, "generated-id").unwrap();
    let msg = plan.message();

    assert_eq!(plan.state(), TaskState::Pending);
    assert_eq!(plan.next_process_at(), Some(now));
    assert_eq!(plan.unique_lock_ttl(), None);
    assert_eq!(msg.id, "generated-id");
    assert_eq!(msg.r#type, "email:welcome");
    assert_eq!(msg.payload, b"payload");
    assert_eq!(msg.queue, DEFAULT_QUEUE_NAME);
    assert_eq!(msg.retry, DEFAULT_MAX_RETRY);
    assert_eq!(msg.retry, DEFAULT_MAX_RETRY);
    assert_eq!(msg.timeout, duration_seconds(DEFAULT_TIMEOUT));
    assert_eq!(msg.timeout, duration_seconds(DEFAULT_TIMEOUT));
    assert_eq!(msg.deadline, 0);
    assert_eq!(NO_TIMEOUT, Duration::ZERO);
    assert_eq!(unix_seconds(NO_DEADLINE), 0);
}

#[test]
fn enqueue_options_apply_at_enqueue_time() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());

    let plan = EnqueuePlan::from_task_with_options(
        &task,
        EnqueueOptions::new()
            .queue(crate::QueueName::new("critical").unwrap())
            .task_id(crate::TaskId::new("enqueue-option-id").unwrap())
            .max_retries(3)
            .timeout(Duration::from_secs(20)),
        now,
        "generated-id",
    )
    .unwrap();
    let msg = plan.message();

    assert_eq!(msg.queue, "critical");
    assert_eq!(msg.id, "enqueue-option-id");
    assert_eq!(msg.retry, 3);
    assert_eq!(msg.timeout, 20);
}

#[test]
fn max_retry_uses_upstream_proto_int32_conversion() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());

    let plan = EnqueuePlan::from_task_with_options(
        &task,
        EnqueueOptions::new().max_retries(2_147_483_648),
        now,
        "generated-id",
    )
    .unwrap();

    assert_eq!(plan.message().retry, i32::MIN);
}

#[test]
fn max_retry_preserves_wide_go_int_before_proto_conversion() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());

    let plan = EnqueuePlan::from_task_with_options(
        &task,
        EnqueueOptions::new().max_retries(4_294_967_296),
        now,
        "generated-id",
    )
    .unwrap();

    assert_eq!(plan.max_retry(), 4_294_967_296);
    assert_eq!(plan.message().retry, 0);
}

#[test]
fn deadline_suppresses_default_timeout() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let deadline = now + Duration::from_secs(600);
    let task = Task::new("email:welcome", Vec::new());

    let plan = EnqueuePlan::from_task_with_options(
        &task,
        EnqueueOptions::new().deadline(deadline),
        now,
        "generated-id",
    )
    .unwrap();

    assert_eq!(plan.message().deadline, unix_seconds(deadline));
    assert_eq!(plan.message().timeout, 0);
}

#[test]
fn unix_epoch_deadline_matches_upstream_no_deadline_sentinel() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());

    let plan = EnqueuePlan::from_task_with_options(
        &task,
        EnqueueOptions::new().deadline(UNIX_EPOCH),
        now,
        "generated-id",
    )
    .unwrap();

    assert_eq!(plan.message().deadline, 0);
    assert_eq!(plan.message().timeout, duration_seconds(DEFAULT_TIMEOUT));
}
