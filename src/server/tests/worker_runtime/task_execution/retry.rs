use super::*;

#[tokio::test]
async fn keeps_handler_result_written_before_task_failure() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let message = message("task-id");
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message))],
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        HandlerFunc(|_task: &Task, context: &ProcessingContext| {
            context.write_result(b"handler-result".to_vec()).unwrap();
            Err(HandlerError::failed("boom"))
        }),
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
            retry_at: now + Duration::from_secs(60)
        }
    );
    assert_eq!(
        assembly.broker.results,
        [(
            "critical".to_owned(),
            "task-id".to_owned(),
            b"handler-result".to_vec()
        )]
    );
    assert_eq!(assembly.broker.retried.len(), 1);
}

#[tokio::test]
async fn failure_hooks_receive_plain_task_view_like_asynq() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut message = message("task-id");
    message.headers.insert("trace-id".into(), "abc".into());
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message))],
        ..RecordingBroker::default()
    };
    let retry_delay = ObservingRetryDelay::new(Duration::from_secs(60));
    let retry_tasks = Arc::clone(&retry_delay.tasks);
    let error_handler = RecordingErrorHandler::default();
    let error_tasks = Arc::clone(&error_handler.tasks);
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        HandlerFunc(|task: &Task, context: &ProcessingContext| {
            assert!(task.header("trace-id").is_some());
            assert!(context.result_writer().is_some());
            assert!(context.deadline().is_some());
            Err::<(), HandlerError>(HandlerError::failed("boom"))
        }),
        retry_delay,
        TestClock(now),
    )
    .with_error_handler(error_handler);

    let result = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        result,
        WorkerRun::Retried {
            task_id: "task-id".to_owned(),
            retry_at: now + Duration::from_secs(60)
        }
    );
    let expected_error_tasks = [ObservedHookTask {
        has_result_writer: true,
        deadline: Some(now + Duration::from_secs(1_800)),
        is_cancelled: false,
        task_id: Some("task-id".to_owned()),
        queue_name: Some("critical".to_owned()),
        retry_count: Some(0),
        max_retry: Some(3),
        trace_id: Some("abc".to_owned()),
    }];
    let expected_retry_tasks = [ObservedHookTask {
        has_result_writer: false,
        deadline: None,
        is_cancelled: false,
        task_id: None,
        queue_name: None,
        retry_count: None,
        max_retry: None,
        trace_id: Some("abc".to_owned()),
    }];
    assert_eq!(&*error_tasks.lock().unwrap(), &expected_error_tasks);
    assert_eq!(&*retry_tasks.lock().unwrap(), &expected_retry_tasks);
}

#[tokio::test]
async fn retries_failed_task_with_configured_delay() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let message = message("task-id");
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message))],
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Err(HandlerError::failed("boom")),
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
            retry_at: now + Duration::from_secs(60)
        }
    );
    assert_eq!(
        assembly.broker.retried,
        [(
            "task-id".to_owned(),
            now + Duration::from_secs(60),
            "boom".to_owned(),
            true
        )]
    );
}

#[tokio::test]
async fn retry_errors_do_not_fail_failed_task() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let runtime_logger: Arc<dyn Logger> = logger.clone();
    let message = message("task-id");
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message))],
        retry_error: Some(RetryError::Other("redis down".to_owned())),
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Err(HandlerError::failed("boom")),
        |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
        TestClock(now),
    )
    .with_logger(runtime_logger);

    let result = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        result,
        WorkerRun::Retried {
            task_id: "task-id".to_owned(),
            retry_at: now + Duration::from_secs(60)
        }
    );
    assert_eq!(assembly.broker.retried.len(), 1);
    assert_eq!(assembly.pending_sync_count(), 1);
    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        [
            "Could not move task id=task-id from \"asynq:{critical}:active\" to \"asynq:{critical}:retry\"; Will retry syncing"
        ]
    );
}
