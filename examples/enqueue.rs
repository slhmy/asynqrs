use std::{env, error::Error, time::Duration};

use asynqrs::{EnqueueOptions, QueueName, RedisBackedClient, Task};

const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1:6379/0";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let redis_url = env::var("ASYNQ_RS_REDIS_URL").unwrap_or_else(|_| DEFAULT_REDIS_URL.to_owned());

    // Reference: Asynq v0.26.0 `Client.Enqueue` quick-start shape:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/README.md#quickstart>.
    let redis = redis::Client::open(redis_url)?;
    let mut client = RedisBackedClient::from_redis_client(redis).await?;

    let payload = br#"{"user_id":42,"template_id":"welcome"}"#.to_vec();
    let task = Task::new("email:welcome", payload);

    let info = client
        .enqueue_with_async(
            &task,
            EnqueueOptions::new()
                .queue(QueueName::new("emails")?)
                .max_retries(5)
                .timeout(Duration::from_secs(30))
                .unique_for(Duration::from_secs(60))
                .process_in(Duration::from_secs(5)),
        )
        .await?;

    println!(
        "enqueued task id={} type={} queue={} state={:?}",
        info.id(),
        info.task_type(),
        info.queue(),
        info.state()
    );

    client.close()?;
    Ok(())
}
