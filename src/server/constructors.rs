//! Server construction helpers.

use crate::aggregation::AggregationBroker;

use super::defaults::{
    DEFAULT_SERVER_FORWARDER_INTERVAL, DEFAULT_SERVER_HEALTH_CHECK_INTERVAL,
    DEFAULT_SERVER_IDLE_SLEEP, DEFAULT_SERVER_JANITOR_INTERVAL,
    DEFAULT_SERVER_METADATA_HEARTBEAT_INTERVAL, DEFAULT_SERVER_RECOVERER_INTERVAL,
    DEFAULT_SERVER_SHUTDOWN_TIMEOUT, DEFAULT_SERVER_SYNCER_INTERVAL,
};
use super::{
    Config, LogLevel, QueueSelector, Server, ServerError, ServerRuntimeStateAttach, Sleeper,
};

mod redis;

pub use redis::{RedisBackedServer, RedisBackedServerBuilder};

impl<P, S> Server<P, S> {
    fn from_queue_selector_parts(
        runtime: P,
        queue_selector: QueueSelector,
        sleeper: S,
        runtime_state: super::ServerRuntimeState,
    ) -> Self {
        let queues = queue_selector.queue_names();
        // Reference: Asynq v0.26.0 `NewServerFromRedisClient` allocates a
        // server with normalized queue selection before applying optional
        // lifecycle config fields:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L431-L620>.
        Self {
            runtime,
            worker_count: 1,
            queues: std::sync::Arc::from(queues),
            queue_selector,
            aggregation_config: None,
            idle_sleep: DEFAULT_SERVER_IDLE_SLEEP,
            forwarder_interval: DEFAULT_SERVER_FORWARDER_INTERVAL,
            recoverer_interval: DEFAULT_SERVER_RECOVERER_INTERVAL,
            janitor_interval: DEFAULT_SERVER_JANITOR_INTERVAL,
            syncer_interval: DEFAULT_SERVER_SYNCER_INTERVAL,
            shutdown_timeout: DEFAULT_SERVER_SHUTDOWN_TIMEOUT,
            health_check_interval: DEFAULT_SERVER_HEALTH_CHECK_INTERVAL,
            health_check_handler: None,
            log_level: LogLevel::Info,
            logger: None,
            metadata: None,
            metadata_heartbeat_interval: DEFAULT_SERVER_METADATA_HEARTBEAT_INTERVAL,
            cancellation_listener: None,
            aggregation_runner: None,
            shared_connection: false,
            sleeper,
            runtime_state,
            state: super::ServerState::New,
        }
    }

    pub fn with_config(mut runtime: P, config: Config, sleeper: S) -> Result<Self, ServerError>
    where
        P: ServerRuntimeStateAttach,
    {
        // Reference: Asynq v0.26.0 `NewServerFromRedisClient` normalizes
        // `Config` before wiring server lifecycle components:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L449-L603>.
        let runtime_state = super::ServerRuntimeState::default();
        runtime.attach_server_runtime(&runtime_state);
        Ok(Self::from_queue_selector_parts(
            runtime,
            config.queue_selector()?,
            sleeper,
            runtime_state,
        ))
        .map(|mut server| {
            server = server
                .with_worker_count(config.effective_concurrency())
                .with_idle_sleep(config.effective_task_check_interval())
                .with_forwarder_interval(config.effective_delayed_task_check_interval())
                .with_shutdown_timeout(config.effective_shutdown_timeout())
                .with_health_check_interval(config.effective_health_check_interval())
                .with_optional_health_check_handler(config.health_check_func.clone())
                .with_log_level(config.effective_log_level())
                .with_optional_logger(config.logger.clone())
                .with_janitor_interval(config.effective_janitor_interval())
                .warn_for_large_janitor_batch_size(config.janitor_batch_size);
            if let Some(aggregation_config) = config.aggregation_config_if_enabled() {
                server = server.with_aggregation_config(aggregation_config);
            }
            server
        })
    }

    pub fn with_config_and_aggregation_broker<B, R>(
        runtime: P,
        config: Config,
        sleeper: S,
        aggregation_broker: B,
        aggregation_sleeper: R,
    ) -> Result<Self, ServerError>
    where
        P: ServerRuntimeStateAttach,
        B: AggregationBroker + Clone + Send + 'static,
        R: Sleeper + Send + 'static,
    {
        // Reference: Asynq v0.26.0 `NewServerFromRedisClient` wires
        // `Config.GroupAggregator` into the server-owned aggregator when
        // aggregation is configured:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L458-L465>.
        let group_aggregator = config.group_aggregator.clone();
        let server = Self::with_config(runtime, config, sleeper)?;
        if let Some(group_aggregator) = group_aggregator {
            server.with_group_aggregator(aggregation_broker, group_aggregator, aggregation_sleeper)
        } else {
            Ok(server)
        }
    }
}
