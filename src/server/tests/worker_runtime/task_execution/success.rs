use super::*;

#[tokio::test]
async fn completes_successful_task() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let message = message("task-id");
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message))],
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        DefaultRetryDelay,
        TestClock(now),
    );

    let result = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        result,
        WorkerRun::Completed {
            task_id: "task-id".to_owned()
        }
    );
    assert_eq!(assembly.broker.completed, ["task-id"]);
}

#[tokio::test]
async fn complete_errors_do_not_fail_successful_task() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let logger = Arc::new(RecordingLogger::default());
    let runtime_logger: Arc<dyn Logger> = logger.clone();
    let message = message("task-id");
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message))],
        complete_error: Some(CompleteError::Other("redis down".to_owned())),
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        |_task: &Task| Ok::<(), HandlerError>(()),
        DefaultRetryDelay,
        TestClock(now),
    )
    .with_logger(runtime_logger);

    let result = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        result,
        WorkerRun::Completed {
            task_id: "task-id".to_owned()
        }
    );
    assert_eq!(assembly.broker.completed, ["task-id"]);
    assert_eq!(assembly.pending_sync_count(), 1);
    assert_eq!(
        logger.logs.lock().unwrap().as_slice(),
        [
            "Could not remove task id=task-id type=\"email:welcome\" from \"asynq:{critical}:active\" err: redis down; Will retry syncing"
        ]
    );
}

#[tokio::test]
async fn writes_handler_result_during_task_processing() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let message = message("task-id");
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message))],
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        HandlerFunc(|_task: &Task, context: &ProcessingContext| {
            assert_eq!(
                context.result_writer().map(|writer| writer.task_id()),
                Some("task-id")
            );
            context.write_result(b"handler-result".to_vec()).unwrap();
            Ok::<(), HandlerError>(())
        }),
        DefaultRetryDelay,
        TestClock(now),
    );

    let result = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        result,
        WorkerRun::Completed {
            task_id: "task-id".to_owned()
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
    assert_eq!(assembly.broker.completed, ["task-id"]);
}

#[tokio::test]
async fn async_result_writer_waits_for_broker_acknowledgement() {
    #[derive(Debug)]
    struct AsyncResultHandler;

    #[async_trait]
    impl Handler for AsyncResultHandler {
        async fn process_task(
            &mut self,
            _task: &Task,
            context: &ProcessingContext,
        ) -> Result<(), HandlerError> {
            let written = context
                .write_result_async(b"handler-result".to_vec())
                .await
                .map_err(|error| HandlerError::failed(error.to_string()))?;
            assert_eq!(written, b"handler-result".len());
            Ok(())
        }
    }

    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let message = message("task-id");
    let broker = RecordingBroker {
        dequeued: vec![Ok(dequeued(message))],
        ..RecordingBroker::default()
    };
    let mut assembly = WorkerAssembly::with_parts(
        broker,
        AsyncResultHandler,
        DefaultRetryDelay,
        TestClock(now),
    );

    let result = run_worker_once(&mut assembly, &["critical".to_owned()])
        .await
        .unwrap();

    assert_eq!(
        result,
        WorkerRun::Completed {
            task_id: "task-id".to_owned()
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
    assert_eq!(assembly.broker.completed, ["task-id"]);
}
