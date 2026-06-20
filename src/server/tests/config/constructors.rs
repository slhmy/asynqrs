use super::*;

#[test]
fn server_with_config_applies_supported_upstream_config_fields() {
    let mut queues = HashMap::new();
    queues.insert("default".to_owned(), 3);
    queues.insert("critical".to_owned(), 6);
    let logger: Arc<dyn Logger> = Arc::new(RecordingLogger::default());
    let health_check_func: Arc<dyn HealthCheckHandler> =
        Arc::new(RecordingHealthCheckHandler::default());
    let config = Config {
        queues,
        concurrency: 4,
        strict_priority: true,
        task_check_interval: Duration::from_secs(2),
        delayed_task_check_interval: Duration::from_secs(3),
        log_level: Some(LogLevel::Warn),
        logger: Some(Arc::clone(&logger)),
        shutdown_timeout: Duration::from_secs(4),
        health_check_func: Some(Arc::clone(&health_check_func)),
        health_check_interval: Duration::from_secs(5),
        group_grace_period: Duration::from_secs(6),
        group_max_delay: Duration::from_secs(7),
        group_max_size: 8,
        janitor_interval: Duration::from_secs(9),
        ..Config::default()
    };

    let server =
        Server::with_config(recording_runtime(), config, RecordingSleeper::default()).unwrap();

    assert_eq!(
        server.queues.as_ref(),
        ["critical".to_owned(), "default".to_owned()]
    );
    assert!(&server.queue_selector.is_strict_priority());
    assert_eq!(server.worker_count, 4);
    assert_eq!(server.idle_sleep, Duration::from_secs(2));
    assert_eq!(server.forwarder_interval, Duration::from_secs(3));
    assert_eq!(server.recoverer_interval, DEFAULT_SERVER_RECOVERER_INTERVAL);
    assert_eq!(server.shutdown_timeout, Duration::from_secs(4));
    assert_eq!(server.health_check_interval, Duration::from_secs(5));
    assert!(server.health_check_handler.is_some());
    assert_eq!(server.log_level, LogLevel::Warn);
    assert!(Arc::ptr_eq(server.logger.as_ref().unwrap(), &logger));
    assert_eq!(server.janitor_interval, Duration::from_secs(9));
    assert_eq!(server.aggregation_config, None);
}

#[test]
fn server_with_config_applies_group_config_only_when_group_aggregator_is_set() {
    let group_aggregator: SharedGroupAggregator =
        Arc::new(tokio::sync::Mutex::new(NoopGroupAggregator));
    let config = Config {
        group_grace_period: Duration::from_secs(6),
        group_max_delay: Duration::from_secs(7),
        group_max_size: 8,
        group_aggregator: Some(group_aggregator),
        ..Config::default()
    };

    let server =
        Server::with_config(recording_runtime(), config, RecordingSleeper::default()).unwrap();

    assert_eq!(
        server.aggregation_config,
        Some(ServerAggregationConfig::new(
            Duration::from_secs(6),
            Duration::from_secs(7),
            8
        ))
    );
}

#[test]
fn server_new_with_config_matches_rust_config_constructor_shape() {
    let server =
        test_support::server_new_with_config(recording_runtime(), Config::default()).unwrap();

    assert_eq!(server.queues.as_ref(), [DEFAULT_QUEUE_NAME.to_owned()]);
    assert!(server.worker_count >= 1);
    assert_eq!(server.idle_sleep, DEFAULT_SERVER_IDLE_SLEEP);
}

#[test]
fn config_builder_builds_rust_native_config_shape() {
    let logger: Arc<dyn Logger> = Arc::new(RecordingLogger::default());
    let health_check_func: Arc<dyn HealthCheckHandler> =
        Arc::new(RecordingHealthCheckHandler::default());

    let config = Config::builder()
        .concurrency(4)
        .queue(crate::QueueName::new("critical").unwrap(), 6usize)
        .queue(crate::QueueName::new("default").unwrap(), 3usize)
        .strict_priority()
        .task_check_interval(Duration::from_secs(2))
        .delayed_task_check_interval(Duration::from_secs(3))
        .log_level(LogLevel::Warn)
        .shared_logger(Arc::clone(&logger))
        .shutdown_timeout(Duration::from_secs(4))
        .shared_health_check_handler(Arc::clone(&health_check_func))
        .health_check_interval(Duration::from_secs(5))
        .group_grace_period(Duration::from_secs(6))
        .group_max_delay(Duration::from_secs(7))
        .group_max_size(8)
        .janitor_interval(Duration::from_secs(9))
        .janitor_batch_size(10)
        .build();

    assert_eq!(config.concurrency(), 4);
    assert_eq!(
        config.queues(),
        &HashMap::from([("critical".to_owned(), 6), ("default".to_owned(), 3)])
    );
    assert!(config.strict_priority());
    assert_eq!(config.task_check_interval(), Duration::from_secs(2));
    assert_eq!(config.delayed_task_check_interval(), Duration::from_secs(3));
    assert_eq!(config.log_level(), Some(LogLevel::Warn));
    assert!(Arc::ptr_eq(config.logger().unwrap(), &logger));
    assert_eq!(config.shutdown_timeout(), Duration::from_secs(4));
    assert!(Arc::ptr_eq(
        config.health_check_func().unwrap(),
        &health_check_func
    ));
    assert_eq!(config.health_check_interval(), Duration::from_secs(5));
    assert_eq!(config.group_grace_period(), Duration::from_secs(6));
    assert_eq!(config.group_max_delay(), Duration::from_secs(7));
    assert_eq!(config.group_max_size(), 8);
    assert_eq!(config.janitor_interval(), Duration::from_secs(9));
    assert_eq!(config.janitor_batch_size(), 10);
}

#[test]
fn config_builder_accepts_owned_logger_and_health_check_callback() {
    let config = Config::builder()
        .logger(RecordingLogger::default())
        .health_check_fn(|_result| {})
        .build();

    assert!(config.logger().is_some());
    assert!(config.health_check_func().is_some());
}

#[test]
fn config_builder_try_queue_validates_queue_name_early() {
    let error = Config::builder().try_queue(" ", 1usize).unwrap_err();

    assert_eq!(error, crate::QueueNameError);
}

#[test]
fn config_builder_try_build_rejects_subsecond_group_grace_period() {
    let error = Config::builder()
        .group_grace_period(Duration::from_millis(500))
        .try_build()
        .unwrap_err();

    assert_eq!(error, ConfigBuildError::GroupGracePeriodTooShort);
    assert_eq!(
        error.to_string(),
        "group grace period cannot be less than a second"
    );
}

#[test]
fn server_accepts_config_builder_output() {
    let config = Config::builder()
        .concurrency(2)
        .try_queue("critical", 6usize)
        .unwrap()
        .try_queue("default", 3usize)
        .unwrap()
        .strict_priority()
        .build();

    let server =
        Server::with_config(recording_runtime(), config, RecordingSleeper::default()).unwrap();

    assert_eq!(
        server.queues.as_ref(),
        ["critical".to_owned(), "default".to_owned()]
    );
    assert_eq!(server.worker_count, 2);
    assert!(&server.queue_selector.is_strict_priority());
}

#[test]
fn redis_backed_server_builder_from_redis_runtime_client_matches_shared_constructor_shape() {
    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();

    let builder = RedisBackedServerBuilder::from_redis_runtime_client(
        RedisRuntimeClient::direct(redis_client),
        Config::default(),
    );
    assert!(builder.shared_connection);
}

#[test]
fn redis_backed_server_builder_new_owns_runtime_client() {
    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();

    let builder =
        RedisBackedServerBuilder::new(RedisRuntimeClient::direct(redis_client), Config::default());

    assert!(!builder.shared_connection);
}

#[test]
fn redis_backed_server_builder_from_direct_redis_client_keeps_direct_client_convenience() {
    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();

    let builder =
        RedisBackedServerBuilder::from_direct_redis_client(redis_client, Config::default());
    assert!(builder.shared_connection);
}

#[test]
fn redis_backed_server_builder_from_redis_runtime_client_accepts_shared_runtime_boundary() {
    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();

    let builder = RedisBackedServerBuilder::from_redis_runtime_client(
        RedisRuntimeClient::direct(redis_client),
        Config::default(),
    );
    assert!(builder.shared_connection);
}

#[tokio::test]
async fn redis_backed_server_constructors_track_owned_and_shared_connections_like_upstream() {
    let shared_client = redis::Client::open("redis://localhost:6379").unwrap();
    let shared = RedisBackedServerBuilder::from_redis_runtime_client(
        RedisRuntimeClient::direct(shared_client),
        Config::default(),
    );
    assert!(shared.shared_connection);

    let owned_client = redis::Client::open("redis://localhost:6379").unwrap();
    let owned = RedisBackedServerBuilder::from_redis_client(owned_client, Config::default());
    assert!(owned.shared_connection);
}

#[test]
fn redis_backed_server_builder_from_redis_client_accepts_redis_rs_client() {
    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();
    let builder = RedisBackedServerBuilder::from_redis_client(redis_client, Config::default());
    assert!(builder.shared_connection);
}

#[test]
fn redis_backed_server_builder_accepts_handler_at_run_and_start_boundary() {
    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();
    let builder =
        RedisBackedServerBuilder::new(RedisRuntimeClient::direct(redis_client), Config::default());

    let run = builder
        .clone()
        .run(|_task: &crate::Task| Ok::<(), crate::HandlerError>(()));
    std::mem::drop(run);

    let start = builder.start(|_task: &crate::Task| Ok::<(), crate::HandlerError>(()));
    std::mem::drop(start);
}

#[tokio::test]
async fn redis_backed_server_builder_rejects_nil_handler_like_upstream_start() {
    type HandlerFn = fn(&crate::Task) -> Result<(), crate::HandlerError>;

    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();
    let builder =
        RedisBackedServerBuilder::new(RedisRuntimeClient::direct(redis_client), Config::default());

    let run_error = builder
        .clone()
        .run_optional_for_test(None::<HandlerFn>)
        .await
        .unwrap_err();
    let start_error = builder
        .start_optional_for_test(None::<HandlerFn>)
        .await
        .unwrap_err();

    assert_eq!(
        run_error,
        ServerConstructionError::Server(ServerError::NilHandler)
    );
    assert_eq!(
        start_error,
        ServerConstructionError::Server(ServerError::NilHandler)
    );
    assert_eq!(
        run_error.to_string(),
        "asynq: server cannot run with nil handler"
    );
}

trait RedisBackedServerBuilderTestExt {
    async fn run_optional_for_test<H>(
        self,
        handler: Option<H>,
    ) -> Result<ServerRunSummary, ServerConstructionError>
    where
        H: crate::Handler + Send;

    async fn start_optional_for_test<H>(
        self,
        handler: Option<H>,
    ) -> Result<ServerHandle, ServerConstructionError>
    where
        H: crate::Handler + Clone + Send + 'static;
}

impl RedisBackedServerBuilderTestExt for RedisBackedServerBuilder {
    async fn run_optional_for_test<H>(
        self,
        handler: Option<H>,
    ) -> Result<ServerRunSummary, ServerConstructionError>
    where
        H: crate::Handler + Send,
    {
        let Some(handler) = handler else {
            return Err(ServerConstructionError::Server(ServerError::NilHandler));
        };
        self.run(handler).await
    }

    async fn start_optional_for_test<H>(
        self,
        handler: Option<H>,
    ) -> Result<ServerHandle, ServerConstructionError>
    where
        H: crate::Handler + Clone + Send + 'static,
    {
        let Some(handler) = handler else {
            return Err(ServerConstructionError::Server(ServerError::NilHandler));
        };
        self.start(handler).await
    }
}

#[test]
fn redis_backed_server_builder_build_with_handler_keeps_rust_constructor_compatibility() {
    fn assert_future<F, H>(_future: F)
    where
        F: std::future::Future<Output = Result<RedisBackedServer<H>, ServerConstructionError>>,
    {
    }

    assert_future(
        RedisBackedServerBuilder::from_redis_client(
            redis::Client::open("redis://localhost:6379").unwrap(),
            Config::default(),
        )
        .build_with_handler(|_task: &crate::Task| Ok::<(), crate::HandlerError>(())),
    );
}
