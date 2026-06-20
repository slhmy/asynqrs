use super::*;

#[test]
fn builds_unique_key_and_lock_ttl() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", b"payload".to_vec());

    let plan = EnqueuePlan::from_task_with_options(
        &task,
        EnqueueOptions::new()
            .queue(crate::QueueName::new("critical").unwrap())
            .unique_for(Duration::from_secs(300)),
        now,
        "generated-id",
    )
    .unwrap();

    assert_eq!(
        plan.message().unique_key,
        "asynq:{critical}:unique:email:welcome:321c3cf486ed509164edec1e1981fec8"
    );
    assert_eq!(plan.unique_lock_ttl(), Some(Duration::from_secs(300)));
}

#[test]
fn builds_unique_key_for_empty_payload_like_upstream_empty_slice_payload() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());

    let plan = EnqueuePlan::from_task_with_options(
        &task,
        EnqueueOptions::new()
            .queue(crate::QueueName::new("critical").unwrap())
            .unique_for(Duration::from_secs(300)),
        now,
        "generated-id",
    )
    .unwrap();

    assert_eq!(
        plan.message().unique_key,
        "asynq:{critical}:unique:email:welcome:d41d8cd98f00b204e9800998ecf8427e"
    );
}

#[test]
fn scheduled_unique_lock_ttl_covers_delay_and_uniqueness_ttl() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", b"payload".to_vec());

    let plan = EnqueuePlan::from_task_with_options(
        &task,
        EnqueueOptions::new()
            .process_in(Duration::from_secs(60))
            .unique_for(Duration::from_secs(300)),
        now,
        "generated-id",
    )
    .unwrap();

    assert_eq!(plan.state(), TaskState::Scheduled);
    assert_eq!(plan.unique_lock_ttl(), Some(Duration::from_secs(360)));
}

#[test]
fn scheduled_unique_lock_ttl_uses_later_schedule_time() {
    let option_now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let enqueue_now = option_now + Duration::from_secs(5);
    let unique_ttl_now = option_now + Duration::from_secs(10);
    let task = Task::new("email:welcome", b"payload".to_vec());

    let plan = EnqueuePlan::from_task_with_options_at(
        &task,
        EnqueueOptions::new()
            .process_in(Duration::from_secs(60))
            .unique_for(Duration::from_secs(300)),
        option_now,
        enqueue_now,
        unique_ttl_now,
        "generated-id",
    )
    .unwrap();

    assert_eq!(plan.state(), TaskState::Scheduled);
    assert_eq!(plan.unique_lock_ttl(), Some(Duration::from_secs(350)));
}
