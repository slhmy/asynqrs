use super::*;

fn context(task_id: &str, queue: &str) -> ProcessingContext {
    ProcessingContext::for_task(
        None,
        tokio_util::sync::CancellationToken::new(),
        task_id.to_owned(),
        queue.to_owned(),
        2,
        25,
    )
}

#[tokio::test]
async fn task_handler_func_adapter_ignores_processing_context() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let handler_calls = Arc::clone(&calls);
    let mut handler = TaskHandlerFunc(move |task: &Task| {
        handler_calls
            .lock()
            .expect("calls poisoned")
            .push(task.type_name().to_owned());
        Ok(())
    });
    let context = context("task-id", "critical");

    Handler::process_task(
        &mut handler,
        &Task::new("email:welcome", Vec::new()),
        &context,
    )
    .await
    .unwrap();

    assert_eq!(
        calls.lock().expect("calls poisoned").as_slice(),
        ["email:welcome"]
    );
}

#[tokio::test]
async fn handler_func_receives_task_and_processing_context() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let handler_calls = Arc::clone(&calls);
    let mut handler = HandlerFunc(move |task: &Task, context: &ProcessingContext| {
        handler_calls.lock().expect("calls poisoned").push(format!(
            "{}:{}:{}",
            context.task_id(),
            context.queue_name(),
            task.type_name()
        ));
        Ok(())
    });
    let context = context("task-id", "critical");

    Handler::process_task(
        &mut handler,
        &Task::new("email:welcome", Vec::new()),
        &context,
    )
    .await
    .unwrap();

    assert_eq!(
        calls.lock().expect("calls poisoned").as_slice(),
        ["task-id:critical:email:welcome"]
    );
}

#[tokio::test]
async fn task_only_closure_implements_handler() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let handler_calls = Arc::clone(&calls);
    let mut handler = move |task: &Task| {
        handler_calls
            .lock()
            .expect("calls poisoned")
            .push(task.type_name().to_owned());
        Ok::<(), HandlerError>(())
    };
    let context = context("task-id", "critical");

    Handler::process_task(
        &mut handler,
        &Task::new("email:welcome", Vec::new()),
        &context,
    )
    .await
    .unwrap();

    assert_eq!(
        calls.lock().expect("calls poisoned").as_slice(),
        ["email:welcome"]
    );
}

#[test]
fn handler_func_inherent_process_task_uses_rust_argument_order() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let handler_calls = Arc::clone(&calls);
    let mut handler = HandlerFunc(move |task: &Task, context: &ProcessingContext| {
        handler_calls.lock().expect("calls poisoned").push(format!(
            "{}:{}",
            task.type_name(),
            context.task_id()
        ));
        Ok(())
    });
    let context = context("task-id", "critical");

    handler
        .process_task(&Task::new("email:welcome", Vec::new()), &context)
        .unwrap();

    assert_eq!(
        calls.lock().expect("calls poisoned").as_slice(),
        ["email:welcome:task-id"]
    );
}

#[tokio::test]
async fn error_handler_func_receives_task_context_and_error() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let handler_calls = Arc::clone(&calls);
    let mut handler = ErrorHandlerFunc(
        move |task: &Task, context: &ProcessingContext, error: &HandlerError| {
            handler_calls.lock().expect("calls poisoned").push((
                task.type_name().to_owned(),
                context.task_id().to_owned(),
                context.queue_name().to_owned(),
                error.to_string(),
            ));
        },
    );
    let context = context("task-id", "critical");

    ErrorHandler::handle_error(
        &mut handler,
        &Task::new("email:welcome", Vec::new()),
        &context,
        &HandlerError::failed("boom"),
    )
    .await;

    assert_eq!(
        calls.lock().expect("calls poisoned").as_slice(),
        [(
            "email:welcome".to_owned(),
            "task-id".to_owned(),
            "critical".to_owned(),
            "boom".to_owned()
        )]
    );
}

#[test]
fn error_handler_func_inherent_handle_error_uses_rust_argument_order() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let handler_calls = Arc::clone(&calls);
    let mut handler = ErrorHandlerFunc(
        move |task: &Task, context: &ProcessingContext, error: &HandlerError| {
            handler_calls.lock().expect("calls poisoned").push((
                task.type_name().to_owned(),
                context.task_id().to_owned(),
                error.to_string(),
            ));
        },
    );
    let context = context("task-id", "critical");

    handler.handle_error(
        &Task::new("email:welcome", Vec::new()),
        &context,
        &HandlerError::failed("boom"),
    );

    assert_eq!(
        calls.lock().expect("calls poisoned").as_slice(),
        [(
            "email:welcome".to_owned(),
            "task-id".to_owned(),
            "boom".to_owned()
        )]
    );
}
