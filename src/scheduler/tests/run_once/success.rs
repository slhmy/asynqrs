use super::*;

#[tokio::test]
async fn run_once_enqueues_due_entries_and_records_metadata() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
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
            EnqueueOptions::new().queue(crate::QueueName::new("critical").unwrap()),
        )
        .unwrap();
    scheduler.entries[0].next_enqueue_at = now;

    let run = scheduler.run_once().await.unwrap();

    assert_eq!(run.enqueued(), 1);
    assert_eq!(scheduler.broker.enqueued.len(), 1);
    assert_eq!(scheduler.broker.events.len(), 1);
    assert_eq!(scheduler.broker.metadata.len(), 1);
    let metadata_entry = &scheduler.broker.metadata[0].1[0];
    assert_eq!(metadata_entry.0, "entry-id");
    let metadata = pb::asynq::SchedulerEntry::decode(metadata_entry.1.as_slice()).unwrap();
    assert_eq!(metadata.id, "entry-id");
    assert_eq!(scheduler.entries.as_slice()[0].spec(), "@every 1m");
    assert_eq!(metadata.spec, "@every 1m");
    assert_eq!(metadata.task_type, "email:welcome");
    assert_eq!(metadata.task_payload, b"payload");
    assert_eq!(metadata.enqueue_options, ["Queue(\"critical\")"]);
    assert_eq!(
        metadata.next_enqueue_time,
        Some(timestamp(now + Duration::from_secs(60)))
    );
    assert_eq!(metadata.prev_enqueue_time, Some(timestamp(now)));

    let event =
        pb::asynq::SchedulerEnqueueEvent::decode(scheduler.broker.events[0].1.as_slice()).unwrap();
    assert_eq!(event.task_id, "entry-id:1700000000");
    assert_eq!(event.enqueue_time, Some(timestamp(now)));
    assert_eq!(scheduler.entries.as_slice()[0].prev_enqueue_at(), Some(now));
    assert_eq!(
        scheduler.entries.as_slice()[0].next_enqueue_at(),
        now + Duration::from_secs(60)
    );
}

#[tokio::test]
async fn run_once_logs_successful_scheduler_enqueue_at_debug_level() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let scheduler_logger: Arc<dyn Logger> = logger.clone();
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap()
    .with_logger(scheduler_logger)
    .with_log_level(LogLevel::Debug);
    scheduler
        .register_spec_with(
            "entry-id",
            Task::new("email:welcome", b"payload".to_vec()),
            "@every 1m",
            EnqueueOptions::new().task_id(crate::TaskId::new("custom-task-id").unwrap()),
        )
        .unwrap();
    scheduler.entries[0].next_enqueue_at = now;

    let run = scheduler.run_once().await.unwrap();

    assert_eq!(run.enqueued(), 1);
    let logs = logger.logs.lock().unwrap();
    assert_eq!(logs.len(), 1);
    assert!(logs[0].starts_with("scheduler enqueued a task: TaskInfo"));
    assert!(logs[0].contains("id: \"custom-task-id\""));
}

#[tokio::test]
async fn run_once_metadata_records_only_register_options() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
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
                .max_retries(5)
                .timeout(Duration::from_secs(20))
                .retain_for(Duration::from_secs(300)),
        )
        .unwrap();
    scheduler.entries[0].next_enqueue_at = now;

    scheduler.run_once().await.unwrap();

    assert_eq!(
        scheduler.entries.as_slice()[0].options(),
        &EnqueueOptions::new()
            .queue(crate::QueueName::new("critical").unwrap())
            .max_retries(5)
            .timeout(Duration::from_secs(20))
            .retain_for(Duration::from_secs(300))
    );
    let enqueued = scheduler.broker.enqueued[0].message();
    assert_eq!(enqueued.queue, "critical");
    assert_eq!(enqueued.retry, 5);
    assert_eq!(enqueued.timeout, 20);
    assert_eq!(enqueued.retention, 300);

    let metadata_entry = &scheduler.broker.metadata[0].1[0];
    let metadata = pb::asynq::SchedulerEntry::decode(metadata_entry.1.as_slice()).unwrap();
    assert_eq!(metadata.task_type, "email:welcome");
    assert_eq!(metadata.task_payload, b"payload");
    assert_eq!(
        metadata.enqueue_options,
        [
            "Queue(\"critical\")",
            "MaxRetry(5)",
            "Timeout(20s)",
            "Retention(5m0s)"
        ]
    );
}

#[tokio::test]
async fn run_once_records_enqueue_event_with_actual_task_id() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
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
            EnqueueOptions::new().task_id(crate::TaskId::new("custom-task-id").unwrap()),
        )
        .unwrap();
    scheduler.entries[0].next_enqueue_at = now;

    scheduler.run_once().await.unwrap();

    let event =
        pb::asynq::SchedulerEnqueueEvent::decode(scheduler.broker.events[0].1.as_slice()).unwrap();
    assert_eq!(event.task_id, "custom-task-id");
}

#[tokio::test]
async fn run_once_records_enqueue_event_with_fresh_time_like_upstream() {
    let tick_now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let event_now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_002);
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        SequenceClock::new([tick_now, tick_now, event_now]),
    )
    .unwrap();
    scheduler
        .register_spec(
            "entry-id",
            Task::new("email:welcome", b"payload".to_vec()),
            "@every 1m",
        )
        .unwrap();
    scheduler.entries[0].next_enqueue_at = tick_now;

    scheduler.run_once().await.unwrap();

    let event =
        pb::asynq::SchedulerEnqueueEvent::decode(scheduler.broker.events[0].1.as_slice()).unwrap();
    assert_eq!(event.task_id, "entry-id:1700000000");
    assert_eq!(event.enqueue_time, Some(timestamp(event_now)));
    assert_eq!(scheduler.broker.events[0].2, event_now);
    assert_eq!(
        scheduler.entries.as_slice()[0].prev_enqueue_at(),
        Some(tick_now)
    );
    assert_eq!(
        scheduler.entries.as_slice()[0].next_enqueue_at(),
        tick_now + Duration::from_secs(60)
    );
}

#[tokio::test]
async fn run_once_enqueues_due_cron_entry_and_schedules_next_fire_time() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_200);
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
            "*/5 * * * *",
            EnqueueOptions::new().queue(crate::QueueName::new("critical").unwrap()),
        )
        .unwrap();
    scheduler.entries[0].next_enqueue_at = now;

    let run = scheduler.run_once().await.unwrap();

    assert_eq!(run.enqueued(), 1);
    let metadata_entry = &scheduler.broker.metadata[0].1[0];
    let metadata = pb::asynq::SchedulerEntry::decode(metadata_entry.1.as_slice()).unwrap();
    assert_eq!(metadata.spec, "*/5 * * * *");
    assert_eq!(metadata.prev_enqueue_time, Some(timestamp(now)));
    assert_eq!(
        metadata.next_enqueue_time,
        Some(timestamp(
            SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_400)
        ))
    );
    assert_eq!(
        scheduler.entries.as_slice()[0].next_enqueue_at(),
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_400)
    );
}
