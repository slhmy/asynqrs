use super::*;

#[test]
fn task_info_can_be_built_from_enqueue_plan() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", b"payload".to_vec());
    let plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();

    let info = TaskInfo::from_enqueue_plan(&plan);

    assert_eq!(info.id(), "task-id");
    assert_eq!(info.queue(), "default");
    assert_eq!(info.type_name(), "email:welcome");
    assert_eq!(info.payload(), b"payload");
    assert_eq!(info.state(), TaskState::Pending);
    assert_eq!(info.next_process_at(), Some(now));
    assert!(!info.is_orphaned());
    assert!(info.result().is_empty());
}

#[test]
fn enqueue_task_info_preserves_pre_encode_max_retry() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", b"payload".to_vec());
    let plan = EnqueuePlan::from_task_with_options(
        &task,
        EnqueueOptions::new().max_retries(4_294_967_296),
        now,
        "task-id",
    )
    .unwrap();

    let info = TaskInfo::from_enqueue_plan(&plan);

    assert_eq!(plan.message().retry, 0);
    assert_eq!(info.max_retry(), 4_294_967_296);
}

#[test]
fn enqueue_task_info_exposes_composed_task_metadata() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let deadline = now + Duration::from_secs(600);
    let task = Task::with_headers("email:welcome", b"payload".to_vec(), [("trace-id", "abc")]);
    let plan = EnqueuePlan::from_task_with_options(
        &task,
        EnqueueOptions::new()
            .queue(crate::QueueName::new("critical").unwrap())
            .task_id(crate::TaskId::new("custom-id").unwrap())
            .max_retries(7)
            .deadline(deadline)
            .retain_for(Duration::from_secs(300))
            .group(crate::GroupName::new("tenant-a").unwrap()),
        now,
        "generated-id",
    )
    .unwrap();

    let info = TaskInfo::from_enqueue_plan(&plan);

    assert_eq!(info.id(), "custom-id");
    assert_eq!(info.queue(), "critical");
    assert_eq!(info.type_name(), "email:welcome");
    assert_eq!(info.payload(), b"payload");
    assert_eq!(
        info.headers().get("trace-id").map(String::as_str),
        Some("abc")
    );
    assert_eq!(info.state(), TaskState::Aggregating);
    assert_eq!(info.max_retry(), 7);
    assert_eq!(info.retried(), 0);
    assert_eq!(info.last_error(), "");
    assert_eq!(info.last_failed_at(), None);
    assert_eq!(info.timeout(), Duration::ZERO);
    assert_eq!(info.deadline(), Some(deadline));
    assert_eq!(info.group(), "tenant-a");
    assert_eq!(info.next_process_at(), None);
    assert!(!info.is_orphaned());
    assert_eq!(info.retention(), Duration::from_secs(300));
    assert_eq!(info.completed_at(), None);
    assert!(info.result().is_empty());
}
