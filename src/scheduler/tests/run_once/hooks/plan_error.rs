use super::*;

#[tokio::test]
async fn run_once_invokes_enqueue_plan_error_hook_on_plan_errors() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let calls = Arc::new(Mutex::new(Vec::new()));
    let observer = Arc::clone(&calls);
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap()
    .with_enqueue_plan_error_hook(move |entry, error| {
        observer
            .lock()
            .unwrap()
            .push((entry.id().to_owned(), error.to_string()));
    });
    scheduler
        .register_spec("entry-id", Task::new(" ", b"payload".to_vec()), "@every 1m")
        .unwrap();
    scheduler.entries[0].next_enqueue_at = now;

    let run = scheduler.run_once().await.unwrap();

    assert_eq!(run.enqueued(), 0);
    assert_eq!(
        calls.lock().unwrap().as_slice(),
        [(
            "entry-id".to_owned(),
            "task typename cannot be empty".to_owned()
        )]
    );
    assert!(scheduler.broker.enqueued.is_empty());
    assert!(scheduler.broker.events.is_empty());
    assert_eq!(scheduler.entries.as_slice()[0].prev_enqueue_at(), Some(now));
    assert_eq!(
        scheduler.entries.as_slice()[0].next_enqueue_at(),
        now + Duration::from_secs(60)
    );
    assert_eq!(scheduler.broker.metadata.len(), 1);
}
