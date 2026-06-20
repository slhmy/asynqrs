use super::super::fixtures::RecordingMiddleware;
use super::*;

#[tokio::test]
async fn serve_mux_use_layers_runs_task_middleware_in_upstream_order() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut mux = ServeMux::new();

    mux.use_layers([
        RecordingMiddleware {
            name: "first",
            calls: Arc::clone(&calls),
        },
        RecordingMiddleware {
            name: "second",
            calls: Arc::clone(&calls),
        },
    ]);

    let handler_calls = Arc::clone(&calls);
    mux.handle_fn("email", move |task: &Task, _context: &ProcessingContext| {
        handler_calls
            .lock()
            .expect("calls poisoned")
            .push(format!("handler:{}", task.type_name()));
        Ok(())
    });

    mux.process_task(
        &Task::new("email:welcome", Vec::new()),
        &test_processing_context(),
    )
    .await
    .unwrap();

    assert_eq!(
        calls.lock().expect("calls poisoned").as_slice(),
        [
            "first:before:email:welcome",
            "second:before:email:welcome",
            "handler:email:welcome",
            "second:after:email:welcome",
            "first:after:email:welcome",
        ]
    );
}

#[tokio::test]
async fn serve_mux_handler_method_returns_middleware_wrapped_handler() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut mux = ServeMux::new();

    mux.use_layers([
        RecordingMiddleware {
            name: "first",
            calls: Arc::clone(&calls),
        },
        RecordingMiddleware {
            name: "second",
            calls: Arc::clone(&calls),
        },
    ]);

    let handler_calls = Arc::clone(&calls);
    mux.handle_fn("email", move |task: &Task, _context: &ProcessingContext| {
        handler_calls
            .lock()
            .expect("calls poisoned")
            .push(format!("handler:{}", task.type_name()));
        Ok(())
    });

    let task = Task::new("email:welcome", Vec::new());
    let (mut handler, pattern) = mux.handler(&task);

    assert_eq!(pattern, "email");
    handler
        .process_task(&task, &test_processing_context())
        .await
        .unwrap();
    assert_eq!(
        calls.lock().expect("calls poisoned").as_slice(),
        [
            "first:before:email:welcome",
            "second:before:email:welcome",
            "handler:email:welcome",
            "second:after:email:welcome",
            "first:after:email:welcome",
        ]
    );
}

#[tokio::test]
async fn serve_mux_middleware_preserves_explicit_handler_context() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut mux = ServeMux::new();

    mux.use_layer(RecordingMiddleware {
        name: "middleware",
        calls: Arc::clone(&calls),
    });

    let handler_calls = Arc::clone(&calls);
    mux.handle_fn("email", move |task: &Task, context: &ProcessingContext| {
        handler_calls.lock().expect("calls poisoned").push(format!(
            "handler:{}:{}",
            context.queue_name(),
            task.type_name()
        ));
        Ok(())
    });

    let context = ProcessingContext::for_task(
        None,
        tokio_util::sync::CancellationToken::new(),
        "task-id".to_owned(),
        "critical".to_owned(),
        0,
        25,
    );
    let task = Task::new("email:welcome", Vec::new());

    Handler::process_task(&mut mux, &task, &context)
        .await
        .unwrap();

    assert_eq!(
        calls.lock().expect("calls poisoned").as_slice(),
        [
            "middleware:before:email:welcome",
            "handler:critical:email:welcome",
            "middleware:after:email:welcome",
        ]
    );
}

#[tokio::test]
async fn serve_mux_builder_layers_middleware_closures_in_order() {
    let calls = Arc::new(Mutex::new(Vec::new()));

    let first_calls = Arc::clone(&calls);
    let second_calls = Arc::clone(&calls);
    let handler_calls = Arc::clone(&calls);
    let mut mux = ServeMux::new()
        .layer_fn(move |task: &Task, context: &ProcessingContext| {
            first_calls.lock().expect("calls poisoned").push(format!(
                "first:{}:{}",
                context.queue_name(),
                task.type_name()
            ));
            Ok(())
        })
        .layer(task_middleware_fn(
            move |task: &Task, _context: &ProcessingContext| {
                second_calls
                    .lock()
                    .expect("calls poisoned")
                    .push(format!("second:{}", task.type_name()));
                Ok(())
            },
        ))
        .route_fn("email", move |task: &Task, context: &ProcessingContext| {
            handler_calls.lock().expect("calls poisoned").push(format!(
                "handler:{}:{}",
                context.queue_name(),
                task.type_name()
            ));
            Ok(())
        });

    let context = ProcessingContext::for_task(
        None,
        tokio_util::sync::CancellationToken::new(),
        "task-id".to_owned(),
        "critical".to_owned(),
        0,
        25,
    );
    mux.process_task(&Task::new("email:welcome", Vec::new()), &context)
        .await
        .unwrap();

    assert_eq!(
        calls.lock().expect("calls poisoned").as_slice(),
        [
            "first:critical:email:welcome",
            "second:email:welcome",
            "handler:critical:email:welcome",
        ]
    );
}

#[tokio::test]
async fn serve_mux_builder_layer_hooks_runs_before_and_after_in_order() {
    let calls = Arc::new(Mutex::new(Vec::new()));

    let first_before_calls = Arc::clone(&calls);
    let first_after_calls = Arc::clone(&calls);
    let second_before_calls = Arc::clone(&calls);
    let second_after_calls = Arc::clone(&calls);
    let handler_calls = Arc::clone(&calls);
    let mut mux = ServeMux::new()
        .layer_hooks(
            move |task: &Task, _context: &ProcessingContext| {
                first_before_calls
                    .lock()
                    .expect("calls poisoned")
                    .push(format!("first:before:{}", task.type_name()));
                Ok(())
            },
            move |task: &Task, _context: &ProcessingContext, result| {
                first_after_calls
                    .lock()
                    .expect("calls poisoned")
                    .push(format!(
                        "first:after:{}:{}",
                        task.type_name(),
                        result.is_ok()
                    ));
                result
            },
        )
        .layer_hooks(
            move |task: &Task, _context: &ProcessingContext| {
                second_before_calls
                    .lock()
                    .expect("calls poisoned")
                    .push(format!("second:before:{}", task.type_name()));
                Ok(())
            },
            move |task: &Task, _context: &ProcessingContext, result| {
                second_after_calls
                    .lock()
                    .expect("calls poisoned")
                    .push(format!(
                        "second:after:{}:{}",
                        task.type_name(),
                        result.is_ok()
                    ));
                result
            },
        )
        .route_fn("email", move |task: &Task, _context: &ProcessingContext| {
            handler_calls
                .lock()
                .expect("calls poisoned")
                .push(format!("handler:{}", task.type_name()));
            Ok(())
        });

    mux.process_task(
        &Task::new("email:welcome", Vec::new()),
        &test_processing_context(),
    )
    .await
    .unwrap();

    assert_eq!(
        calls.lock().expect("calls poisoned").as_slice(),
        [
            "first:before:email:welcome",
            "second:before:email:welcome",
            "handler:email:welcome",
            "second:after:email:welcome:true",
            "first:after:email:welcome:true",
        ]
    );
}
