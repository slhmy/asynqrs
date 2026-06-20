use super::*;

const REDIS_URL_ENV: &str = "ASYNQ_RS_REDIS_URL";
const REDIS_STRICT_ENV: &str = "ASYNQ_RS_REDIS_STRICT";
const REDIS_CONTAINER_START_ATTEMPTS: usize = 3;

pub(super) struct RedisFixture {
    pub(super) _container: Option<Container<Redis>>,
    pub(super) url: String,
    pub(super) connection: redis::Connection,
    pub(super) queue: String,
}

impl RedisFixture {
    pub(super) fn new(name: &str) -> Option<Self> {
        let (url, container) = redis_url()?;
        let client = match redis::Client::open(url.as_ref()) {
            Ok(client) => client,
            Err(error) => {
                skip_or_panic(format!(
                    "failed to open configured Redis URL from {REDIS_URL_ENV} ({error})"
                ));
                return None;
            }
        };
        let connection = match client.get_connection() {
            Ok(connection) => connection,
            Err(error) => {
                skip_or_panic(format!(
                    "failed to connect to configured Redis URL from {REDIS_URL_ENV} ({error})"
                ));
                return None;
            }
        };
        let queue = format!("asynq-rs-test-{name}-{}", uuid::Uuid::new_v4().simple());
        let mut fixture = Self {
            _container: container,
            url,
            connection,
            queue,
        };
        fixture.cleanup();
        Some(fixture)
    }

    pub(super) async fn enqueue_with(&self, task: &Task, options: EnqueueOptions) -> EnqueueResult {
        let mut broker = self.async_broker().await;
        let plan = EnqueuePlan::from_task_with_options(
            task,
            options,
            SystemTime::now(),
            uuid::Uuid::new_v4().to_string(),
        )
        .unwrap();
        broker.enqueue(&plan).await.unwrap();
        EnqueueResult::from_enqueue_plan(&plan)
    }

    pub(super) fn enqueue_options(&self, task_id: &str) -> EnqueueOptions {
        EnqueueOptions::new()
            .queue(crate::QueueName::new(self.queue.clone()).unwrap())
            .task_id(crate::TaskId::new(task_id).unwrap())
    }

    pub(super) async fn async_broker(
        &self,
    ) -> RedisBroker<RedisConnectionExecutor<redis::aio::MultiplexedConnection>> {
        let redis_client = redis::Client::open(self.url.as_ref()).unwrap();
        let connection = redis_client
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let executor = RedisConnectionExecutor::new(connection);
        RedisBroker::new(executor)
    }

    pub(super) fn queue(&self) -> &str {
        &self.queue
    }

    pub(super) fn task_key(&self, task_id: &str) -> String {
        format!("{}{}", self.task_key_prefix(), task_id)
    }

    pub(super) fn task_key_prefix(&self) -> String {
        format!("asynq:{{{}}}:t:", self.queue)
    }

    pub(super) fn pending_key(&self) -> String {
        format!("asynq:{{{}}}:pending", self.queue)
    }

    pub(super) fn paused_key(&self) -> String {
        format!("asynq:{{{}}}:paused", self.queue)
    }

    pub(super) fn active_key(&self) -> String {
        format!("asynq:{{{}}}:active", self.queue)
    }

    pub(super) fn scheduled_key(&self) -> String {
        format!("asynq:{{{}}}:scheduled", self.queue)
    }

    pub(super) fn lease_key(&self) -> String {
        format!("asynq:{{{}}}:lease", self.queue)
    }

    pub(super) fn completed_key(&self) -> String {
        format!("asynq:{{{}}}:completed", self.queue)
    }

    pub(super) fn retry_key(&self) -> String {
        format!("asynq:{{{}}}:retry", self.queue)
    }

    pub(super) fn archived_key(&self) -> String {
        format!("asynq:{{{}}}:archived", self.queue)
    }

    pub(super) fn processed_key(&self, time: SystemTime) -> String {
        format!("asynq:{{{}}}:processed:{}", self.queue, utc_date(time))
    }

    pub(super) fn failed_key(&self, time: SystemTime) -> String {
        format!("asynq:{{{}}}:failed:{}", self.queue, utc_date(time))
    }

    pub(super) fn processed_total_key(&self) -> String {
        format!("asynq:{{{}}}:processed", self.queue)
    }

    pub(super) fn failed_total_key(&self) -> String {
        format!("asynq:{{{}}}:failed", self.queue)
    }

    pub(super) fn daily_stat_keys(&mut self, name: &str) -> Vec<String> {
        let pattern = format!("asynq:{{{}}}:{name}:*", self.queue);
        let mut keys: Vec<String> = self.connection.keys(pattern).unwrap();
        keys.sort();
        keys
    }

    pub(super) fn group_key(&self, group: &str) -> String {
        format!("asynq:{{{}}}:g:{group}", self.queue)
    }

    pub(super) fn aggregation_set_key(&self, group: &str, set_id: &str) -> String {
        format!("{}:{set_id}", self.group_key(group))
    }

    pub(super) fn all_aggregation_sets_key(&self) -> String {
        format!("asynq:{{{}}}:aggregation_sets", self.queue)
    }

    fn cleanup(&mut self) {
        let pattern = format!("asynq:{{{}}}:*", self.queue);
        let keys: Vec<String> = self.connection.keys(pattern).unwrap();
        if !keys.is_empty() {
            let _: usize = self.connection.del(keys).unwrap();
        }
        let _: usize = self.connection.srem("asynq:queues", &self.queue).unwrap();
    }

    pub(super) fn clear_runtime_metadata(&mut self, hostname: &str, pid: i32, server_id: &str) {
        let server_key = format!("asynq:servers:{{{hostname}:{pid}:{server_id}}}");
        let workers_key = format!("asynq:workers:{{{hostname}:{pid}:{server_id}}}");
        let _: usize = self
            .connection
            .del(&[server_key.as_str(), workers_key.as_str()])
            .unwrap();
        let _: usize = self.connection.zrem("asynq:servers", &server_key).unwrap();
        let _: usize = self.connection.zrem("asynq:workers", &workers_key).unwrap();
    }
}

impl Drop for RedisFixture {
    fn drop(&mut self) {
        self.cleanup();
    }
}

fn redis_url() -> Option<(String, Option<Container<Redis>>)> {
    if let Ok(url) = std::env::var(REDIS_URL_ENV) {
        return Some((url, None));
    }

    match start_redis_container() {
        Ok((url, container)) => Some((url, Some(container))),
        Err(error) => {
            skip_or_panic(format!(
                "set {REDIS_URL_ENV} or make Docker available ({error})"
            ));
            None
        }
    }
}

fn start_redis_container() -> Result<(String, Container<Redis>), String> {
    let mut errors = Vec::new();
    for attempt in 1..=REDIS_CONTAINER_START_ATTEMPTS {
        match start_redis_container_once() {
            Ok(started) => return Ok(started),
            Err(error) => errors.push(format!("attempt {attempt}: {error}")),
        }
    }
    Err(errors.join("; "))
}

fn start_redis_container_once() -> Result<(String, Container<Redis>), String> {
    let container = Redis::default()
        .start()
        .map_err(|error| format!("failed to start Redis container ({error})"))?;
    let host = container
        .get_host()
        .map_err(|error| format!("failed to resolve container host ({error})"))?;
    let port = container
        .get_host_port_ipv4(REDIS_PORT)
        .map_err(|error| format!("failed to resolve Redis port ({error})"))?;
    Ok((format!("redis://{host}:{port}"), container))
}

fn skip_or_panic(message: String) {
    if strict_redis_smoke() {
        panic!("Redis integration test required but unavailable: {message}");
    }
    eprintln!("skipping Redis integration test: {message}");
}

fn strict_redis_smoke() -> bool {
    std::env::var(REDIS_STRICT_ENV)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "on"))
        .unwrap_or(false)
}
