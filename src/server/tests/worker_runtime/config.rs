use super::*;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn worker_assembly_applies_configured_processing_scope() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let base_deadline = now + Duration::from_secs(30);
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_probe = Arc::clone(&calls);
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message("task-id")))],
        ..RecordingBroker::default()
    };
    let assembly = WorkerAssembly::with_parts(
        broker,
        HandlerFunc(move |_task: &Task, context: &ProcessingContext| {
            assert_eq!(context.deadline(), Some(base_deadline));
            assert_eq!(context.task_id(), "task-id");
            assert_eq!(context.queue_name(), "critical");
            assert_eq!(context.retry_count(), 0);
            assert_eq!(context.max_retry(), 3);
            calls_probe.fetch_add(1, Ordering::Relaxed);
            Ok::<(), HandlerError>(())
        }),
        DefaultRetryDelay,
        TestClock(now),
    );
    let config = Config {
        processing_scope: Some(Arc::new(move || {
            ProcessingScope::new(Some(base_deadline), CancellationToken::new())
        })),
        ..Config::default()
    };

    let mut assembly = assembly.apply_server_config(&config);
    let result = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        result,
        WorkerRun::Completed {
            task_id: "task-id".to_owned(),
        }
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn worker_assembly_preserves_live_processing_scope_cancellation() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let scope_token = Arc::new(Mutex::new(None::<CancellationToken>));
    let scope_token_for_context = Arc::clone(&scope_token);
    let saw_cancelled = Arc::new(AtomicUsize::new(0));
    let saw_cancelled_probe = Arc::clone(&saw_cancelled);
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message("task-id")))],
        ..RecordingBroker::default()
    };
    let assembly = WorkerAssembly::with_parts(
        broker,
        HandlerFunc(move |_task: &Task, context: &ProcessingContext| {
            assert!(!context.is_cancelled());
            scope_token.lock().unwrap().as_ref().unwrap().cancel();
            if context.is_cancelled() {
                saw_cancelled_probe.fetch_add(1, Ordering::Relaxed);
            }
            Ok::<(), HandlerError>(())
        }),
        DefaultRetryDelay,
        TestClock(now),
    );
    let config = Config {
        processing_scope: Some(Arc::new(move || {
            let token = CancellationToken::new();
            *scope_token_for_context.lock().unwrap() = Some(token.clone());
            ProcessingScope::new(None, token)
        })),
        ..Config::default()
    };

    let mut assembly = assembly.apply_server_config(&config);
    let result = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        result,
        WorkerRun::Completed {
            task_id: "task-id".to_owned(),
        }
    );
    assert_eq!(saw_cancelled.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn worker_assembly_retries_cancelled_processing_scope() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_probe = Arc::clone(&calls);
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message("task-id")))],
        ..RecordingBroker::default()
    };
    let assembly = WorkerAssembly::with_parts(
        broker,
        move |_task: &Task| {
            calls_probe.fetch_add(1, Ordering::Relaxed);
            Ok::<(), HandlerError>(())
        },
        |_retried: i32, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
        TestClock(now),
    );
    let config = Config {
        processing_scope: Some(Arc::new(|| {
            let token = CancellationToken::new();
            token.cancel();
            ProcessingScope::new(None, token)
        })),
        ..Config::default()
    };

    let mut assembly = assembly.apply_server_config(&config);
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
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert_eq!(assembly.broker.retried[0].2, "context canceled");
}

#[tokio::test]
async fn worker_assembly_retries_expired_processing_scope_deadline() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_probe = Arc::clone(&calls);
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message("task-id")))],
        ..RecordingBroker::default()
    };
    let assembly = WorkerAssembly::with_parts(
        broker,
        move |_task: &Task| {
            calls_probe.fetch_add(1, Ordering::Relaxed);
            Ok::<(), HandlerError>(())
        },
        |_retried: i32, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
        TestClock(now),
    );
    let config = Config {
        processing_scope: Some(Arc::new(move || {
            ProcessingScope::new(Some(now - Duration::from_secs(1)), CancellationToken::new())
        })),
        ..Config::default()
    };

    let mut assembly = assembly.apply_server_config(&config);
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
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert_eq!(assembly.broker.retried[0].2, "context deadline exceeded");
}

#[test]
fn worker_assembly_applies_configured_retry_delay_func() {
    let assembly = WorkerAssembly::with_parts(
        RecordingBroker::default(),
        |_task: &Task| Ok::<(), HandlerError>(()),
        |_retried: i32, _error: &HandlerError, _task: &Task| Duration::from_secs(90),
        TestClock(UNIX_EPOCH),
    );
    let retry_delay = SharedRetryDelay(Arc::new(Mutex::new(RetryDelayFunc(
        |_retried: i32, _error: &HandlerError, _task: &Task| Duration::from_secs(17),
    ))));
    let config = Config {
        retry_delay_func: Some(retry_delay),
        ..Config::default()
    };

    let mut assembly = assembly.apply_server_config(&config);
    let task = Task::new("email:welcome", Vec::new());
    let error = HandlerError::failed("boom");

    assert_eq!(
        assembly.retry_delay_for(1, &error, &task),
        Duration::from_secs(17)
    );
}

#[test]
fn worker_assembly_applies_configured_is_failure_func() {
    let assembly = WorkerAssembly::with_parts(
        RecordingBroker::default(),
        |_task: &Task| Ok::<(), HandlerError>(()),
        |_retried: i32, _error: &HandlerError, _task: &Task| Duration::from_secs(90),
        TestClock(UNIX_EPOCH),
    );
    let is_failure = SharedIsFailure(Arc::new(Mutex::new(|_error: &HandlerError| false)));
    let config = Config {
        is_failure: Some(is_failure),
        ..Config::default()
    };

    let mut assembly = assembly.apply_server_config(&config);
    let error = HandlerError::failed("boom");

    assert!(!assembly.is_failure_for(&error));
}

#[tokio::test]
async fn worker_assembly_applies_configured_error_handler() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let calls_probe = Arc::clone(&calls);
    let assembly = WorkerAssembly::with_parts(
        RecordingBroker::default(),
        |_task: &Task| Ok::<(), HandlerError>(()),
        |_retried: i32, _error: &HandlerError, _task: &Task| Duration::from_secs(90),
        TestClock(UNIX_EPOCH),
    );
    let error_handler = SharedErrorHandler(Arc::new(tokio::sync::Mutex::new(ErrorHandlerFunc(
        move |task: &Task, _context: &ProcessingContext, error: &HandlerError| {
            calls_probe
                .lock()
                .unwrap()
                .push((task.type_name().to_owned(), error.to_string()));
        },
    ))));
    let config = Config {
        error_handler: Some(error_handler),
        ..Config::default()
    };

    let mut assembly = assembly.apply_server_config(&config);
    let task = Task::new("email:welcome", Vec::new());
    let context =
        ProcessingContext::for_task(None, CancellationToken::new(), "task-id", "critical", 0, 3);
    let error = HandlerError::failed("boom");
    assembly.handle_error_for(&task, &context, &error).await;

    assert_eq!(
        calls.lock().unwrap().as_slice(),
        [("email:welcome".to_owned(), "boom".to_owned())]
    );
}
