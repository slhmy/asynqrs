use super::*;

#[tokio::test]
async fn serve_mux_returns_upstream_not_found_error_shape() {
    let mut mux = ServeMux::new();

    let error = mux
        .process_task(
            &Task::new("email:welcome", Vec::new()),
            &test_processing_context(),
        )
        .await
        .unwrap_err();

    assert!(error.is_handler_not_found());
    assert_eq!(
        error.to_string(),
        "handler not found for task \"email:welcome\""
    );
}

#[tokio::test]
async fn not_found_handler_returns_upstream_error_shape() {
    let task = Task::new("email:welcome", Vec::new());
    let mut handler = not_found_handler();

    let direct_error = not_found(&task);
    let handler_error = handler
        .process_task(&task, &test_processing_context())
        .await
        .unwrap_err();

    assert_eq!(
        direct_error.to_string(),
        "handler not found for task \"email:welcome\""
    );
    assert_eq!(handler_error, direct_error);
    assert_eq!(
        HandlerError::HandlerNotFoundSentinel,
        HandlerError::handler_not_found_sentinel()
    );
    assert_eq!(
        HandlerError::HandlerNotFoundSentinel.to_string(),
        "handler not found for task"
    );
    assert!(is_handler_not_found_error(
        &HandlerError::HandlerNotFoundSentinel
    ));
    assert!(is_handler_not_found_error(&handler_error));
    assert!(handler_error.is_handler_not_found());
}

#[test]
#[should_panic(expected = "asynq: invalid pattern")]
fn serve_mux_rejects_blank_patterns_like_upstream() {
    ServeMux::new().handle_fn(" ", |_task: &Task, _context: &ProcessingContext| Ok(()));
}

#[test]
#[should_panic(expected = "asynq: multiple registrations for image")]
fn serve_mux_rejects_duplicate_patterns_like_upstream() {
    let mut mux = ServeMux::new();
    mux.handle_fn("image", |_task: &Task, _context: &ProcessingContext| Ok(()));
    mux.handle_fn("image", |_task: &Task, _context: &ProcessingContext| Ok(()));
}
