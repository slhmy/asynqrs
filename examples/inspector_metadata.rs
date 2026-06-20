use std::{env, error::Error};

use asynqrs::Inspector;

const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1:6379/0";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let redis_url = env::var("ASYNQ_RS_REDIS_URL").unwrap_or_else(|_| DEFAULT_REDIS_URL.to_owned());
    let redis = redis::Client::open(redis_url)?;

    let mut inspector = Inspector::from_redis_client(redis).await?;
    let servers = inspector.servers().await?;
    println!("servers={}", servers.len());
    inspector.close()?;
    Ok(())
}
