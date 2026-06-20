use super::*;

#[tokio::test]
async fn run_once_invokes_enqueue_error_hook_after_post_hook() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let calls = Arc::new(Mutex::new(Vec::new()));
    let post_observer = Arc::clone(&calls);
    let error_observer = Arc::clone(&calls);
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker {
            enqueue_error: Some(BrokerError::Other("redis down".to_owned())),
            ..RecordingSchedulerBroker::default()
        },
        TestClock(now),
    )
    .unwrap()
    .with_post_enqueue_hook(move |entry, plan, result| {
        post_observer.lock().unwrap().push(format!(
            "post:{}:{}:{}",
            entry.id(),
            plan.message().id,
            result
                .err()
                .map(ToString::to_string)
                .unwrap_or_else(|| "ok".to_owned())
        ));
    })
    .with_enqueue_error_hook(move |entry, plan, error| {
        error_observer.lock().unwrap().push(format!(
            "error:{}:{}:{}",
            entry.id(),
            plan.message().id,
            error
        ));
    });
    scheduler
        .register_spec(
            "entry-id",
            Task::new("email:welcome", b"payload".to_vec()),
            "@every 1m",
        )
        .unwrap();
    scheduler.entries[0].next_enqueue_at = now;

    let run = scheduler.run_once().await.unwrap();

    assert_eq!(run.enqueued(), 0);
    assert_eq!(
        calls.lock().unwrap().as_slice(),
        [
            "post:entry-id:entry-id:1700000000:redis down".to_owned(),
            "error:entry-id:entry-id:1700000000:redis down".to_owned()
        ]
    );
    assert!(scheduler.broker.events.is_empty());
    assert_eq!(scheduler.broker.metadata.len(), 1);
}
