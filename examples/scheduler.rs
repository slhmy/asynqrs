use std::{env, error::Error};

use asynqrs::{EnqueueOptions, QueueName, RedisBackedScheduler, SchedulerOpts, Task};

const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1:6379/0";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let redis_url = env::var("ASYNQ_RS_REDIS_URL").unwrap_or_else(|_| DEFAULT_REDIS_URL.to_owned());
    let redis = redis::Client::open(redis_url)?;

    let mut scheduler =
        RedisBackedScheduler::from_redis_client(redis, SchedulerOpts::default()).await?;
    let entry_id = scheduler.register_spec_with_generated_id_and(
        Task::new("email:digest", b"{}".to_vec()),
        "@every 1m",
        EnqueueOptions::new().queue(QueueName::new("emails")?),
    )?;

    println!("registered scheduler entry {entry_id}");
    Ok(())
}
