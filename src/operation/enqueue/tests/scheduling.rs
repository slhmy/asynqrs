use super::*;

#[test]
fn schedules_future_process_time() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let process_at = now + Duration::from_secs(60);
    let task = Task::new("email:welcome", Vec::new());

    let plan = EnqueuePlan::from_task_with_options(
        &task,
        EnqueueOptions::new().process_at(process_at),
        now,
        "generated-id",
    )
    .unwrap();

    assert_eq!(plan.state(), TaskState::Scheduled);
    assert_eq!(plan.next_process_at(), Some(process_at));
}

#[test]
fn process_in_uses_planning_time() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());

    let plan = EnqueuePlan::from_task_with_options(
        &task,
        EnqueueOptions::new().process_in(Duration::from_secs(90)),
        now,
        "generated-id",
    )
    .unwrap();

    assert_eq!(plan.state(), TaskState::Scheduled);
    assert_eq!(plan.next_process_at(), Some(now + Duration::from_secs(90)));
}

#[test]
fn process_in_due_by_enqueue_time_is_pending() {
    let option_now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let enqueue_now = option_now + Duration::from_nanos(1);
    let task = Task::new("email:welcome", Vec::new());

    let plan = EnqueuePlan::from_task_with_options_at(
        &task,
        EnqueueOptions::new().process_in(Duration::from_nanos(1)),
        option_now,
        enqueue_now,
        enqueue_now,
        "task-id",
    )
    .unwrap();

    assert_eq!(plan.state(), TaskState::Pending);
    assert_eq!(plan.next_process_at(), Some(enqueue_now));
}

#[test]
fn sends_due_group_tasks_to_aggregating_state() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());

    let plan = EnqueuePlan::from_task_with_options(
        &task,
        EnqueueOptions::new().group(crate::GroupName::new("tenant-a").unwrap()),
        now,
        "generated-id",
    )
    .unwrap();
    let msg = plan.message();

    assert_eq!(plan.state(), TaskState::Aggregating);
    assert_eq!(plan.next_process_at(), None);
    assert_eq!(msg.group_key, "tenant-a");
}

#[test]
fn scheduled_state_takes_priority_over_group() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let process_at = now + Duration::from_secs(60);
    let task = Task::new("email:welcome", Vec::new());

    let plan = EnqueuePlan::from_task_with_options(
        &task,
        EnqueueOptions::new()
            .group(crate::GroupName::new("tenant-a").unwrap())
            .process_at(process_at),
        now,
        "generated-id",
    )
    .unwrap();

    assert_eq!(plan.state(), TaskState::Scheduled);
    assert_eq!(plan.next_process_at(), Some(process_at));
    assert_eq!(plan.message().group_key, "tenant-a");
}
