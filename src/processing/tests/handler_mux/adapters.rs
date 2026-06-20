use super::*;
use crate::{TaskPayloadError, TypedTaskPayload};

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

#[derive(Debug, PartialEq, Eq)]
struct WelcomePayload {
    user_id: u64,
}

impl TypedTaskPayload for WelcomePayload {
    const TASK_TYPE: &'static str = "email:welcome";

    fn encode_payload(self) -> Result<Vec<u8>, TaskPayloadError> {
        Ok(self.user_id.to_string().into_bytes())
    }

    fn decode_payload(bytes: &[u8]) -> Result<Self, TaskPayloadError> {
        let user_id = std::str::from_utf8(bytes)
            .map_err(|error| TaskPayloadError::Decode(error.to_string()))?
            .parse()
            .map_err(|error: std::num::ParseIntError| {
                TaskPayloadError::Decode(error.to_string())
            })?;
        Ok(Self { user_id })
    }
}

#[tokio::test]
async fn typed_handler_func_decodes_payload_and_preserves_context() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let handler_calls = Arc::clone(&calls);
    let mut handler = typed_handler::<WelcomePayload, _>(
        move |payload: WelcomePayload, context: &ProcessingContext| {
            handler_calls.lock().expect("calls poisoned").push(format!(
                "{}:{}",
                payload.user_id,
                context.task_id()
            ));
            Ok(())
        },
    );
    let task = WelcomePayload { user_id: 42 }.into_task().unwrap();
    let context = context("task-id", "critical");

    Handler::process_task(&mut handler, &task, &context)
        .await
        .unwrap();

    assert_eq!(
        calls.lock().expect("calls poisoned").as_slice(),
        ["42:task-id"]
    );
}

#[tokio::test]
async fn typed_handler_func_maps_decode_failure_to_handler_error() {
    let mut handler = typed_handler::<WelcomePayload, _>(
        |_payload: WelcomePayload, _context: &ProcessingContext| Ok(()),
    );
    let context = context("task-id", "critical");

    let error = Handler::process_task(
        &mut handler,
        &Task::new("email:welcome", b"not-a-number".to_vec()),
        &context,
    )
    .await
    .unwrap_err();

    assert!(
        matches!(error, HandlerError::Failed(message) if message.contains("failed to decode typed payload"))
    );
}

#[tokio::test]
async fn typed_handler_func_rejects_mismatched_task_type() {
    let mut handler = typed_handler::<WelcomePayload, _>(
        |_payload: WelcomePayload, _context: &ProcessingContext| Ok(()),
    );
    let context = context("task-id", "critical");

    let error = Handler::process_task(
        &mut handler,
        &Task::new("email:other", b"42".to_vec()),
        &context,
    )
    .await
    .unwrap_err();

    assert!(
        matches!(error, HandlerError::Failed(message) if message.contains("expected task type"))
    );
}

#[tokio::test]
async fn serve_mux_registers_typed_handler_by_payload_task_type() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let handler_calls = Arc::clone(&calls);
    let mut mux = ServeMux::new().route_typed::<WelcomePayload, _>(
        move |payload: WelcomePayload, _context: &ProcessingContext| {
            handler_calls
                .lock()
                .expect("calls poisoned")
                .push(payload.user_id);
            Ok(())
        },
    );
    let task = WelcomePayload { user_id: 7 }.into_task().unwrap();
    let context = context("task-id", "critical");
    let pattern = mux.matching_pattern("email:welcome").map(str::to_owned);

    Handler::process_task(&mut mux, &task, &context)
        .await
        .unwrap();

    assert_eq!(pattern.as_deref(), Some("email:welcome"));
    assert_eq!(calls.lock().expect("calls poisoned").as_slice(), [7]);
}

#[cfg(feature = "macros")]
#[tokio::test]
async fn serve_mux_macro_registers_multiple_typed_handlers() {
    #[derive(Debug, PartialEq, Eq)]
    struct ResizePayload {
        width: u32,
    }

    impl TypedTaskPayload for ResizePayload {
        const TASK_TYPE: &'static str = "image:resize";

        fn encode_payload(self) -> Result<Vec<u8>, TaskPayloadError> {
            Ok(self.width.to_string().into_bytes())
        }

        fn decode_payload(bytes: &[u8]) -> Result<Self, TaskPayloadError> {
            let width = std::str::from_utf8(bytes)
                .map_err(|error| TaskPayloadError::Decode(error.to_string()))?
                .parse()
                .map_err(|error: std::num::ParseIntError| {
                    TaskPayloadError::Decode(error.to_string())
                })?;
            Ok(Self { width })
        }
    }

    let calls = Arc::new(Mutex::new(Vec::new()));
    let welcome_calls = Arc::clone(&calls);
    let resize_calls = Arc::clone(&calls);
    let mut mux = crate::serve_mux! {
        WelcomePayload => move |payload: WelcomePayload, _context: &ProcessingContext| {
            welcome_calls
                .lock()
                .expect("calls poisoned")
                .push(format!("welcome:{}", payload.user_id));
            Ok(())
        },
        ResizePayload => move |payload: ResizePayload, _context: &ProcessingContext| {
            resize_calls
                .lock()
                .expect("calls poisoned")
                .push(format!("resize:{}", payload.width));
            Ok(())
        },
    };
    let context = context("task-id", "critical");

    Handler::process_task(
        &mut mux,
        &WelcomePayload { user_id: 9 }.into_task().unwrap(),
        &context,
    )
    .await
    .unwrap();
    Handler::process_task(
        &mut mux,
        &ResizePayload { width: 128 }.into_task().unwrap(),
        &context,
    )
    .await
    .unwrap();

    assert_eq!(
        calls.lock().expect("calls poisoned").as_slice(),
        ["welcome:9", "resize:128"]
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
