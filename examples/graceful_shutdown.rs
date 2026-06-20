use std::{env, error::Error, time::Duration};

use async_trait::async_trait;
use asynqrs::{Config, Handler, HandlerError, ProcessingContext, RedisBackedServerBuilder, Task};

const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1:6379/0";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let redis_url = env::var("ASYNQ_RS_REDIS_URL").unwrap_or_else(|_| DEFAULT_REDIS_URL.to_owned());
    let redis = redis::Client::open(redis_url)?;

    let server = RedisBackedServerBuilder::from_redis_client(
        redis,
        Config::builder()
            .concurrency(2)
            .try_queue("default", 1usize)?
            .shutdown_timeout(Duration::from_secs(5))
            .try_build()?,
    );

    let handle = server.start(DemoHandler).await?;
    handle.stop().await?;
    let summary = handle.shutdown().await?;
    println!("completed={}", summary.completed());
    Ok(())
}

#[derive(Clone)]
struct DemoHandler;

#[async_trait]
impl Handler for DemoHandler {
    async fn process_task(
        &mut self,
        _task: &Task,
        _context: &ProcessingContext,
    ) -> Result<(), HandlerError> {
        Ok(())
    }
}
