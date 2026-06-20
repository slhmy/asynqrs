use std::{env, error::Error, str, time::Duration};

use asynqrs::{Config, HandlerError, ProcessingContext, RedisBackedServerBuilder, ServeMux, Task};

const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1:6379/0";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let redis_url = env::var("ASYNQ_RS_REDIS_URL").unwrap_or_else(|_| DEFAULT_REDIS_URL.to_owned());

    let config = Config::builder()
        .concurrency(4)
        .try_queue("emails", 1usize)?
        .shutdown_timeout(Duration::from_secs(10))
        .try_build()?;

    // Reference: Asynq v0.26.0 `Server.Run` quick-start shape:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/README.md#quickstart>.
    let redis = redis::Client::open(redis_url)?;
    let server = RedisBackedServerBuilder::from_redis_client(redis, config);

    let mux = ServeMux::new()
        .layer_fn(|task: &Task, context: &ProcessingContext| {
            println!(
                "received id={} queue={} type={}",
                context.task_id(),
                context.queue_name(),
                task.task_type()
            );
            Ok(())
        })
        .route_fn("email:welcome", handle_welcome_email);

    println!("processing queue=emails type=email:welcome; press Ctrl-C to stop");
    let summary = server.run(mux).await?;
    println!(
        "server stopped: processed={} completed={}",
        summary.processed(),
        summary.completed()
    );

    Ok(())
}

fn handle_welcome_email(task: &Task, context: &ProcessingContext) -> Result<(), HandlerError> {
    let payload = str::from_utf8(task.payload()).unwrap_or("<non-utf8 payload>");
    println!(
        "handled id={} queue={} type={} retry={}/{} payload={}",
        context.task_id(),
        context.queue_name(),
        task.task_type(),
        context.retry_count(),
        context.max_retry(),
        payload
    );
    Ok(())
}
