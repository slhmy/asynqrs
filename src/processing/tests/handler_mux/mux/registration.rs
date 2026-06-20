use super::*;

#[tokio::test]
async fn serve_mux_handle_methods_match_upstream_names() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut mux = ServeMux::new();

    let handle_calls = Arc::clone(&calls);
    mux.handle("image", move |task: &Task| {
        handle_calls
            .lock()
            .expect("calls poisoned")
            .push(format!("handle:{}", task.type_name()));
        Ok(())
    });

    let handle_func_calls = Arc::clone(&calls);
    mux.handle_fn("email", move |task: &Task, _context: &ProcessingContext| {
        handle_func_calls
            .lock()
            .expect("calls poisoned")
            .push(format!("handle-func:{}", task.type_name()));
        Ok(())
    });

    mux.process_task(
        &Task::new("image:resize", Vec::new()),
        &test_processing_context(),
    )
    .await
    .unwrap();
    mux.process_task(
        &Task::new("email:welcome", Vec::new()),
        &test_processing_context(),
    )
    .await
    .unwrap();

    assert_eq!(
        calls.lock().expect("calls poisoned").as_slice(),
        ["handle:image:resize", "handle-func:email:welcome"]
    );
}

#[tokio::test]
async fn serve_mux_registers_context_handler_funcs_like_upstream_handle_func() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut mux = ServeMux::new();

    let snake_calls = Arc::clone(&calls);
    mux.handle_fn("email", move |task: &Task, context: &ProcessingContext| {
        snake_calls.lock().expect("calls poisoned").push(format!(
            "snake:{}:{}",
            context.queue_name(),
            task.type_name()
        ));
        Ok(())
    });

    let upstream_named_calls = Arc::clone(&calls);
    mux.handle_fn("image", move |task: &Task, context: &ProcessingContext| {
        upstream_named_calls
            .lock()
            .expect("calls poisoned")
            .push(format!("named:{}:{}", context.task_id(), task.type_name()));
        Ok(())
    });

    let email_task = Task::new("email:welcome", Vec::new());
    let email_context = ProcessingContext::for_task(
        None,
        tokio_util::sync::CancellationToken::new(),
        "email-id".to_owned(),
        "critical".to_owned(),
        0,
        25,
    );
    let image_task = Task::new("image:resize", Vec::new());
    let image_context = ProcessingContext::for_task(
        None,
        tokio_util::sync::CancellationToken::new(),
        "image-id".to_owned(),
        "media".to_owned(),
        0,
        25,
    );

    mux.process_task(&email_task, &email_context).await.unwrap();
    mux.process_task(&image_task, &image_context).await.unwrap();

    assert_eq!(
        calls.lock().expect("calls poisoned").as_slice(),
        [
            "snake:critical:email:welcome",
            "named:image-id:image:resize"
        ]
    );
}
