use super::*;

#[tokio::test]
async fn serve_mux_handler_method_returns_matched_handler_and_pattern() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut mux = ServeMux::new();

    let image_calls = Arc::clone(&calls);
    mux.handle_fn("image", move |task: &Task, _context: &ProcessingContext| {
        image_calls
            .lock()
            .expect("calls poisoned")
            .push(format!("image:{}", task.type_name()));
        Ok(())
    });

    let thumbnail_calls = Arc::clone(&calls);
    mux.handle_fn(
        "image:thumbnail",
        move |task: &Task, _context: &ProcessingContext| {
            thumbnail_calls
                .lock()
                .expect("calls poisoned")
                .push(format!("thumbnail:{}", task.type_name()));
            Ok(())
        },
    );

    let task = Task::new("image:thumbnail:resize", Vec::new());
    let (mut handler, pattern) = mux.handler(&task);

    assert_eq!(pattern, "image:thumbnail");
    handler
        .process_task(&task, &test_processing_context())
        .await
        .unwrap();
    assert_eq!(
        calls.lock().expect("calls poisoned").as_slice(),
        ["thumbnail:image:thumbnail:resize"]
    );
}

#[tokio::test]
async fn serve_mux_handler_method_returns_not_found_handler_when_unmatched() {
    let mut mux = ServeMux::new();
    let task = Task::new("email:welcome", Vec::new());
    let (mut handler, pattern) = mux.handler(&task);

    assert_eq!(pattern, "");
    let error = handler
        .process_task(&task, &test_processing_context())
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "handler not found for task \"email:welcome\""
    );
    assert!(error.is_handler_not_found());
}
