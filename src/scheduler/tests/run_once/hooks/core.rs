use super::*;

#[tokio::test]
async fn run_once_invokes_scheduler_enqueue_hooks() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let pre_calls = Arc::new(Mutex::new(Vec::new()));
    let post_calls = Arc::new(Mutex::new(Vec::new()));
    let pre_observer = Arc::clone(&pre_calls);
    let post_observer = Arc::clone(&post_calls);
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap()
    .with_pre_enqueue_hook(move |entry, plan| {
        pre_observer.lock().unwrap().push((
            entry.id().to_owned(),
            plan.message().id.clone(),
            plan.message().queue.clone(),
        ));
    })
    .with_post_enqueue_hook(move |entry, plan, result| {
        post_observer.lock().unwrap().push((
            entry.id().to_owned(),
            plan.message().id.clone(),
            result.is_ok(),
        ));
    });
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
    assert_eq!(
        pre_calls.lock().unwrap().as_slice(),
        [(
            "entry-id".to_owned(),
            "entry-id:1700000000".to_owned(),
            "critical".to_owned()
        )]
    );
    assert_eq!(
        post_calls.lock().unwrap().as_slice(),
        [(
            "entry-id".to_owned(),
            "entry-id:1700000000".to_owned(),
            true
        )]
    );
}

#[tokio::test]
async fn scheduler_options_install_enqueue_hooks() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let pre_calls = Arc::new(Mutex::new(Vec::new()));
    let post_calls = Arc::new(Mutex::new(Vec::new()));
    let pre_observer = Arc::clone(&pre_calls);
    let post_observer = Arc::clone(&post_calls);
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(now),
    )
    .unwrap()
    .with_scheduler_opts(SchedulerOpts {
        pre_enqueue_hook: Some(Arc::new(move |entry, plan| {
            pre_observer.lock().unwrap().push((
                entry.id().to_owned(),
                plan.message().id.clone(),
                plan.message().queue.clone(),
            ));
        })),
        post_enqueue_hook: Some(Arc::new(move |entry, plan, result| {
            post_observer.lock().unwrap().push((
                entry.id().to_owned(),
                plan.message().id.clone(),
                result.is_ok(),
            ));
        })),
        ..SchedulerOpts::default()
    });
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
    assert_eq!(
        pre_calls.lock().unwrap().as_slice(),
        [(
            "entry-id".to_owned(),
            "entry-id:1700000000".to_owned(),
            "critical".to_owned()
        )]
    );
    assert_eq!(
        post_calls.lock().unwrap().as_slice(),
        [(
            "entry-id".to_owned(),
            "entry-id:1700000000".to_owned(),
            true
        )]
    );
}
