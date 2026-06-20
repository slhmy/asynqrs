use super::*;

#[tokio::test]
async fn expired_deadline_retries_without_calling_handler() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut message = message("task-id");
    message.deadline = 1_699_999_999;
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message))],
        ..RecordingBroker::default()
    };
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&handler_calls);
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        move |_task: &Task| {
            calls.fetch_add(1, Ordering::Relaxed);
            Ok::<(), HandlerError>(())
        },
        |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
        TestClock(now),
    );

    let result = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        result,
        WorkerRun::Retried {
            task_id: "task-id".to_owned(),
            retry_at: now + Duration::from_secs(60),
        }
    );
    assert_eq!(handler_calls.load(Ordering::Relaxed), 0);
    assert_eq!(assembly.broker.retried[0].2, "context deadline exceeded");
}

#[test]
fn task_deadline_defaults_when_timeout_and_deadline_are_unset() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut message = message("task-id");
    message.timeout = 0;
    message.deadline = 0;

    let deadline = task_deadline(&message, now).unwrap();

    assert_eq!(deadline, Some(now + DEFAULT_TIMEOUT));
}

#[test]
fn task_deadline_uses_upstream_unix_second_comparison_when_both_set() {
    let now = UNIX_EPOCH + Duration::from_secs(100) + Duration::from_millis(900);
    let mut message = message("task-id");
    message.timeout = 10;
    message.deadline = 111;

    let deadline = task_deadline(&message, now).unwrap();

    assert_eq!(deadline, Some(UNIX_EPOCH + Duration::from_secs(110)));
}

#[test]
fn task_deadline_wraps_timeout_unix_addition_when_both_set() {
    let now = UNIX_EPOCH - Duration::from_secs(100);
    let mut message = message("task-id");
    message.timeout = i64::MIN;
    message.deadline = -50;

    let deadline = task_deadline(&message, now).unwrap();

    assert_eq!(deadline, Some(UNIX_EPOCH - Duration::from_secs(50)));
}

#[test]
fn task_deadline_uses_signed_task_message_timeout() {
    let now = UNIX_EPOCH + Duration::from_secs(100) + Duration::from_millis(900);
    let mut message = message("task-id");
    message.timeout = -10;
    message.deadline = 0;

    let deadline = task_deadline(&message, now).unwrap();

    assert_eq!(
        deadline,
        Some(UNIX_EPOCH + Duration::from_secs(90) + Duration::from_millis(900))
    );

    message.deadline = 95;
    let deadline = task_deadline(&message, now).unwrap();

    assert_eq!(deadline, Some(UNIX_EPOCH + Duration::from_secs(90)));
}

#[tokio::test]
async fn timeout_and_deadline_use_earliest_deadline() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut message = message("task-id");
    message.timeout = 60;
    message.deadline = 1_700_000_001;
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message))],
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        PendingHandler {
            calls: Arc::new(AtomicUsize::new(0)),
        },
        |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
        TestClock(now),
    );

    let queues = ["critical".to_owned()];
    let result = {
        let result = run_worker_once(&mut assembly, &queues);
        tokio::pin!(result);
        tokio::task::yield_now().await;
        result.await.unwrap()
    };

    assert_eq!(
        result,
        WorkerRun::Retried {
            task_id: "task-id".to_owned(),
            retry_at: now + Duration::from_secs(60),
        }
    );
    assert_eq!(assembly.broker.retried[0].2, "context deadline exceeded");
}
