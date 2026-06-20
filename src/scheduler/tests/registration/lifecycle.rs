use super::*;

#[tokio::test]
async fn unregister_removes_entry_from_future_runs() {
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

    scheduler.unregister("entry-id").unwrap();
    let run = scheduler.run_once().await.unwrap();

    assert!(scheduler.entries.as_slice().is_empty());
    assert_eq!(run.enqueued(), 0);
    assert!(scheduler.broker.enqueued.is_empty());
    assert_eq!(scheduler.broker.metadata[0].1.len(), 0);
}

#[test]
fn unregister_reports_missing_entries() {
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap();

    let err = scheduler.unregister(" ").unwrap_err();
    assert_eq!(err, SchedulerError::EntryNotFound(" ".to_owned()));
    assert_eq!(err.to_string(), "asynq: no scheduler entry found");

    let err = scheduler.unregister("missing-entry").unwrap_err();
    assert_eq!(
        err,
        SchedulerError::EntryNotFound("missing-entry".to_owned())
    );
    assert_eq!(err.to_string(), "asynq: no scheduler entry found");
}
