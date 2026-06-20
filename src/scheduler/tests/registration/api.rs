use super::*;

#[test]
fn scheduler_entry_exposes_asynq_field_accessors() {
    let now = SystemTime::UNIX_EPOCH;
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap();

    scheduler
        .register_spec_with(
            "entry-id",
            Task::new("email:welcome", b"payload".to_vec()),
            "@every 1m",
            EnqueueOptions::new()
                .queue(crate::QueueName::new("critical").unwrap())
                .max_retries(3)
                .timeout(Duration::from_secs(30)),
        )
        .unwrap();

    let entry = &mut scheduler.entries[0];
    entry.prev_enqueue_at = Some(now);

    assert_eq!(entry.id(), "entry-id");
    assert_eq!(entry.spec(), "@every 1m");
    assert_eq!(entry.task().type_name(), "email:welcome");
    assert_eq!(entry.task().payload(), b"payload");
    assert_eq!(
        entry.options(),
        &EnqueueOptions::new()
            .queue(crate::QueueName::new("critical").unwrap())
            .max_retries(3)
            .timeout(Duration::from_secs(30))
    );
    assert_eq!(entry.next_enqueue_at(), now + Duration::from_secs(60));
    assert_eq!(entry.prev_enqueue_at(), Some(now));
}

#[test]
fn register_spec_with_generated_id_returns_upstream_style_entry_id() {
    let now = SystemTime::UNIX_EPOCH;
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap();

    let entry_id = scheduler
        .register_spec_with_generated_id(
            Task::new("email:welcome", b"payload".to_vec()),
            "@every 1m",
        )
        .unwrap();

    assert!(uuid::Uuid::parse_str(&entry_id).is_ok());
    assert_eq!(scheduler.entries.as_slice()[0].id(), entry_id);
    scheduler.unregister(&entry_id).unwrap();
    assert!(scheduler.entries.as_slice().is_empty());
}

#[test]
fn register_and_unregister_methods_match_upstream_names() {
    let now = SystemTime::UNIX_EPOCH;
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap();

    let entry_id = scheduler
        .register_spec_with_generated_id(
            Task::new("email:welcome", b"payload".to_vec()),
            "@every 1m",
        )
        .unwrap();

    assert!(uuid::Uuid::parse_str(&entry_id).is_ok());
    assert_eq!(scheduler.entries.as_slice()[0].id(), entry_id);

    scheduler.unregister(&entry_id).unwrap();

    assert!(scheduler.entries.as_slice().is_empty());
}

#[test]
fn register_with_accepts_rust_native_enqueue_options() {
    let now = SystemTime::UNIX_EPOCH;
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap();

    let entry_id = scheduler
        .register_spec_with_generated_id_and(
            Task::new("email:welcome", b"payload".to_vec()),
            "@every 1m",
            EnqueueOptions::new()
                .queue(crate::QueueName::new("critical").unwrap())
                .max_retries(5)
                .timeout(Duration::from_secs(20)),
        )
        .unwrap();

    assert!(uuid::Uuid::parse_str(&entry_id).is_ok());
    let entry = &scheduler.entries.as_slice()[0];
    assert_eq!(entry.id(), entry_id);
    assert_eq!(entry.spec(), "@every 1m");
    assert_eq!(
        entry.options(),
        &EnqueueOptions::new()
            .queue(crate::QueueName::new("critical").unwrap())
            .max_retries(5)
            .timeout(Duration::from_secs(20))
    );
}
