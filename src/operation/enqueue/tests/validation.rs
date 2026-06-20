use super::*;

#[test]
fn validates_enqueue_inputs() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let too_large_duration = Duration::from_nanos(i64::MAX as u64) + Duration::from_nanos(1);

    assert_eq!(
        EnqueuePlan::from_task(&Task::new(" ", Vec::new()), now, "generated-id").unwrap_err(),
        EnqueuePlanError::EmptyTaskType
    );
    assert_eq!(
        EnqueuePlan::from_task_with_options(
            &Task::new("email:welcome", Vec::new()),
            EnqueueOptions {
                queue: Some(" ".to_owned()),
                ..EnqueueOptions::new()
            },
            now,
            "generated-id",
        )
        .unwrap_err(),
        EnqueuePlanError::EmptyQueueName
    );
    assert_eq!(
        EnqueuePlan::from_task_with_options(
            &Task::new("email:welcome", Vec::new()),
            EnqueueOptions {
                task_id: Some(" ".to_owned()),
                ..EnqueueOptions::new()
            },
            now,
            "generated-id",
        )
        .unwrap_err(),
        EnqueuePlanError::EmptyTaskId
    );
    assert_eq!(
        EnqueuePlan::from_task_with_options(
            &Task::new("email:welcome", Vec::new()),
            EnqueueOptions::new().unique_for(Duration::ZERO),
            now,
            "generated-id",
        )
        .unwrap_err(),
        EnqueuePlanError::UniqueTtlTooShort
    );
    assert_eq!(
        EnqueuePlan::from_task_with_options(
            &Task::new("email:welcome", Vec::new()),
            EnqueueOptions::new().timeout(too_large_duration),
            now,
            "generated-id",
        )
        .unwrap_err(),
        EnqueuePlanError::DurationOverflow("timeout")
    );
    assert_eq!(
        EnqueuePlan::from_task_with_options(
            &Task::new("email:welcome", Vec::new()),
            EnqueueOptions::new().unique_for(too_large_duration),
            now,
            "generated-id",
        )
        .unwrap_err(),
        EnqueuePlanError::DurationOverflow("unique ttl")
    );
    assert_eq!(
        EnqueuePlan::from_task_with_options(
            &Task::new("email:welcome", Vec::new()),
            EnqueueOptions::new().process_in(too_large_duration),
            now,
            "generated-id",
        )
        .unwrap_err(),
        EnqueuePlanError::DurationOverflow("process in")
    );
    assert_eq!(
        EnqueuePlan::from_task_with_options(
            &Task::new("email:welcome", Vec::new()),
            EnqueueOptions::new().retain_for(too_large_duration),
            now,
            "generated-id",
        )
        .unwrap_err(),
        EnqueuePlanError::DurationOverflow("retention")
    );
    assert_eq!(
        EnqueuePlan::from_task_with_options(
            &Task::new("email:welcome", Vec::new()),
            EnqueueOptions {
                group: Some(" ".to_owned()),
                ..EnqueueOptions::new()
            },
            now,
            "generated-id",
        )
        .unwrap_err(),
        EnqueuePlanError::EmptyGroupKey
    );
}

#[test]
fn is_blank_matches_upstream_trim_space_helper() {
    assert!(is_blank(""));
    assert!(is_blank(" \t\n"));
    assert!(!is_blank(" task "));
}

#[test]
fn accepts_upstream_valid_queue_names_verbatim() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());

    let plan = EnqueuePlan::from_task_with_options(
        &task,
        EnqueueOptions {
            queue: Some(" MyQueue ".to_owned()),
            ..EnqueueOptions::new()
        },
        now,
        "generated-id",
    )
    .unwrap();

    assert_eq!(plan.message().queue, " MyQueue ");
}

#[test]
fn accepts_upstream_valid_task_id_and_group_verbatim() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());

    let plan = EnqueuePlan::from_task_with_options(
        &task,
        EnqueueOptions {
            task_id: Some(" task-id ".to_owned()),
            group: Some(" tenant-a ".to_owned()),
            ..EnqueueOptions::new()
        },
        now,
        "generated-id",
    )
    .unwrap();

    assert_eq!(plan.message().id, " task-id ");
    assert_eq!(plan.message().group_key, " tenant-a ");
}
