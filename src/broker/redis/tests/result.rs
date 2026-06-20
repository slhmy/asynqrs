use super::*;

#[test]
fn async_write_result_stores_task_result_field() {
    let Some(mut fixture) = RedisFixture::new("async-write-result") else {
        return;
    };
    tokio::runtime::Runtime::new().unwrap().block_on(
        async_write_result_stores_task_result_field_inner(&mut fixture),
    );
}

async fn async_write_result_stores_task_result_field_inner(fixture: &mut RedisFixture) {
    let task = Task::new("email:welcome", b"payload".to_vec());
    fixture
        .enqueue_with(
            &task,
            fixture
                .enqueue_options("task-id")
                .retain_for(Duration::from_secs(300)),
        )
        .await;

    let mut broker = fixture.async_broker().await;
    let written = broker
        .write_result(fixture.queue(), "task-id", b"handler-result".to_vec())
        .await
        .unwrap();

    assert_eq!(written, b"handler-result".len());
    let result: Vec<u8> = fixture
        .connection
        .hget(fixture.task_key("task-id"), "result")
        .unwrap();
    assert_eq!(result, b"handler-result");
}

#[test]
fn async_worker_handler_result_is_stored_before_completion() {
    let Some(mut fixture) = RedisFixture::new("async-handler-result") else {
        return;
    };
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async_worker_handler_result_is_stored_before_completion_inner(&mut fixture));
}

async fn async_worker_handler_result_is_stored_before_completion_inner(fixture: &mut RedisFixture) {
    let task = Task::new("email:result", b"payload".to_vec());
    fixture
        .enqueue_with(
            &task,
            fixture
                .enqueue_options("task-id")
                .retain_for(Duration::from_secs(300)),
        )
        .await;

    let broker = fixture.async_broker().await;
    let mut worker_assembly = WorkerAssembly::with_parts(
        broker,
        HandlerFunc(|_task: &Task, context: &ProcessingContext| {
            context.write_result(b"handler-result".to_vec()).unwrap();
            Ok::<(), HandlerError>(())
        }),
        DefaultRetryDelay,
        SystemClock,
    );

    let result = run_worker_once(&mut worker_assembly, &[fixture.queue().to_owned()])
        .await
        .unwrap();

    assert_eq!(
        result,
        WorkerRun::Completed {
            task_id: "task-id".to_owned()
        }
    );
    wait_for_state(fixture, "task-id", "completed").await;
    let stored_result: Vec<u8> = fixture
        .connection
        .hget(fixture.task_key("task-id"), "result")
        .unwrap();
    assert_eq!(stored_result, b"handler-result");
}

#[test]
fn async_worker_handler_result_written_before_failure_is_stored() {
    let Some(mut fixture) = RedisFixture::new("async-handler-result-failure") else {
        return;
    };
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async_worker_handler_result_written_before_failure_is_stored_inner(&mut fixture));
}

async fn async_worker_handler_result_written_before_failure_is_stored_inner(
    fixture: &mut RedisFixture,
) {
    let task = Task::new("email:result", b"payload".to_vec());
    fixture
        .enqueue_with(&task, fixture.enqueue_options("task-id").max_retries(3))
        .await;

    let broker = fixture.async_broker().await;
    let mut worker_assembly = WorkerAssembly::with_retry_delay(
        broker,
        HandlerFunc(|_task: &Task, context: &ProcessingContext| {
            context.write_result(b"handler-result".to_vec()).unwrap();
            Err(HandlerError::failed("boom"))
        }),
        |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
    );

    let result = run_worker_once(&mut worker_assembly, &[fixture.queue().to_owned()])
        .await
        .unwrap();

    assert!(matches!(result, WorkerRun::Retried { .. }));
    let stored_result: Vec<u8> = fixture
        .connection
        .hget(fixture.task_key("task-id"), "result")
        .unwrap();
    assert_eq!(stored_result, b"handler-result");
    let retry_ids: Vec<String> = fixture
        .connection
        .zrange(fixture.retry_key(), 0, -1)
        .unwrap();
    assert_eq!(retry_ids, ["task-id"]);
}
