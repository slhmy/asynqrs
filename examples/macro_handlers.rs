use asynqrs::{Handler, HandlerError, ProcessingContext, TaskPayload, TypedTaskPayload, serve_mux};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, TaskPayload)]
#[task_type = "email:welcome"]
struct WelcomeEmail {
    user_id: u64,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, TaskPayload)]
#[task_type = "image:resize"]
struct ResizeImage {
    width: u32,
}

fn handle_welcome(payload: WelcomeEmail, context: &ProcessingContext) -> Result<(), HandlerError> {
    assert_eq!(payload.user_id, 42);
    assert_eq!(context.queue_name(), "critical");
    Ok(())
}

fn handle_resize(payload: ResizeImage, context: &ProcessingContext) -> Result<(), HandlerError> {
    assert_eq!(payload.width, 128);
    assert_eq!(context.task_id(), "task-id");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut mux = serve_mux! {
        WelcomeEmail => handle_welcome,
        ResizeImage => handle_resize,
    };
    let context = ProcessingContext::for_task(
        None,
        tokio_util::sync::CancellationToken::new(),
        "task-id",
        "critical",
        0,
        25,
    );

    Handler::process_task(
        &mut mux,
        &WelcomeEmail { user_id: 42 }.into_task()?,
        &context,
    )
    .await?;
    Handler::process_task(&mut mux, &ResizeImage { width: 128 }.into_task()?, &context).await?;

    println!("typed handlers registered with serve_mux!");
    Ok(())
}
