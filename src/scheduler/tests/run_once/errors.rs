use super::*;

#[tokio::test]
async fn run_once_ignores_scheduler_history_write_errors_after_successful_enqueue() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let scheduler_logger: Arc<dyn Logger> = logger.clone();
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker {
            record_event_error: Some(MetadataError::Other("redis down".to_owned())),
            ..RecordingSchedulerBroker::default()
        },
        TestClock(now),
    )
    .unwrap()
    .with_logger(scheduler_logger);
    scheduler
        .register_spec(
            "entry-id",
            Task::new("email:welcome", b"payload".to_vec()),
            "@every 1m",
        )
        .unwrap();
    scheduler.entries[0].next_enqueue_at = now;

    let run = scheduler.run_once().await.unwrap();

    assert_eq!(run.enqueued(), 1);
    assert_eq!(scheduler.broker.enqueued.len(), 1);
    assert!(scheduler.broker.events.is_empty());
    assert_eq!(scheduler.entries.as_slice()[0].prev_enqueue_at(), Some(now));
    assert_eq!(
        scheduler.entries.as_slice()[0].next_enqueue_at(),
        now + Duration::from_secs(60)
    );
    assert_eq!(scheduler.broker.metadata.len(), 1);
    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        [
            "scheduler could not record enqueue event of enqueued task entry-id:1700000000: redis down"
        ]
    );
}

#[tokio::test]
async fn run_once_continues_after_enqueue_error_for_one_entry() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let post_calls = Arc::new(Mutex::new(Vec::new()));
    let post_observer = Arc::clone(&post_calls);
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker {
            enqueue_errors: VecDeque::from([BrokerError::Other("redis down".to_owned())]),
            ..RecordingSchedulerBroker::default()
        },
        TestClock(now),
    )
    .unwrap()
    .with_post_enqueue_hook(move |entry, plan, result| {
        post_observer.lock().unwrap().push((
            entry.id().to_owned(),
            plan.message().id.clone(),
            result.is_ok(),
        ));
    });
    scheduler
        .register_spec(
            "entry-a",
            Task::new("email:welcome", b"payload-a".to_vec()),
            "@every 1m",
        )
        .unwrap();
    scheduler
        .register_spec(
            "entry-b",
            Task::new("email:reminder", b"payload-b".to_vec()),
            "@every 1m",
        )
        .unwrap();
    scheduler.entries[0].next_enqueue_at = now;
    scheduler.entries[1].next_enqueue_at = now;

    let run = scheduler.run_once().await.unwrap();

    assert_eq!(run.enqueued(), 1);
    assert_eq!(scheduler.broker.enqueued.len(), 1);
    assert_eq!(
        scheduler.broker.enqueued[0].message().r#type,
        "email:reminder"
    );
    assert_eq!(scheduler.broker.events.len(), 1);
    assert_eq!(scheduler.broker.events[0].0, "entry-b");
    assert_eq!(
        post_calls.lock().unwrap().as_slice(),
        [
            ("entry-a".to_owned(), "entry-a:1700000000".to_owned(), false),
            ("entry-b".to_owned(), "entry-b:1700000000".to_owned(), true)
        ]
    );
    assert_eq!(scheduler.entries.as_slice()[0].prev_enqueue_at(), Some(now));
    assert_eq!(scheduler.entries.as_slice()[1].prev_enqueue_at(), Some(now));
    assert_eq!(
        scheduler.entries.as_slice()[0].next_enqueue_at(),
        now + Duration::from_secs(60)
    );
    assert_eq!(
        scheduler.entries.as_slice()[1].next_enqueue_at(),
        now + Duration::from_secs(60)
    );
    assert_eq!(scheduler.broker.metadata.len(), 1);
}

#[tokio::test]
async fn run_once_continues_after_enqueue_plan_error_for_one_entry() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap();
    scheduler
        .register_spec(
            "entry-a",
            Task::new(" ", b"payload-a".to_vec()),
            "@every 1m",
        )
        .unwrap();
    scheduler
        .register_spec(
            "entry-b",
            Task::new("email:reminder", b"payload-b".to_vec()),
            "@every 1m",
        )
        .unwrap();
    scheduler.entries[0].next_enqueue_at = now;
    scheduler.entries[1].next_enqueue_at = now;

    let run = scheduler.run_once().await.unwrap();

    assert_eq!(run.enqueued(), 1);
    assert_eq!(scheduler.broker.enqueued.len(), 1);
    assert_eq!(
        scheduler.broker.enqueued[0].message().r#type,
        "email:reminder"
    );
    assert_eq!(scheduler.broker.events.len(), 1);
    assert_eq!(scheduler.broker.events[0].0, "entry-b");
    assert_eq!(scheduler.entries.as_slice()[0].prev_enqueue_at(), Some(now));
    assert_eq!(scheduler.entries.as_slice()[1].prev_enqueue_at(), Some(now));
    assert_eq!(
        scheduler.entries.as_slice()[0].next_enqueue_at(),
        now + Duration::from_secs(60)
    );
    assert_eq!(
        scheduler.entries.as_slice()[1].next_enqueue_at(),
        now + Duration::from_secs(60)
    );
    assert_eq!(scheduler.broker.metadata.len(), 1);
}
