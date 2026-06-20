use super::*;

#[test]
fn server_config_exposes_asynq_field_accessors() {
    let mut queues = HashMap::new();
    queues.insert("default".to_owned(), 3);
    queues.insert("critical".to_owned(), 6);
    let processing_scope: Arc<ServerProcessingScope> = Arc::new(ProcessingScope::default);
    let logger: Arc<dyn Logger> = Arc::new(RecordingLogger::default());
    let health_check_func: Arc<dyn HealthCheckHandler> =
        Arc::new(RecordingHealthCheckHandler::default());
    let group_aggregator: SharedGroupAggregator =
        Arc::new(tokio::sync::Mutex::new(NoopGroupAggregator));
    let retry_delay_func = SharedRetryDelay(Arc::new(StdMutex::new(crate::RetryDelayFunc(
        |_retried: i32, _error: &crate::HandlerError, _task: &crate::Task| Duration::from_secs(11),
    ))));
    let is_failure = SharedIsFailure(Arc::new(StdMutex::new(|_error: &crate::HandlerError| {
        false
    })));
    let error_handler: SharedErrorHandler =
        SharedErrorHandler(Arc::new(Mutex::new(crate::NoopErrorHandler)));
    let config = Config {
        concurrency: 4,
        processing_scope: Some(Arc::clone(&processing_scope)),
        queues,
        strict_priority: true,
        task_check_interval: Duration::from_secs(2),
        retry_delay_func: Some(retry_delay_func.clone()),
        is_failure: Some(is_failure.clone()),
        error_handler: Some(error_handler.clone()),
        log_level: Some(LogLevel::Warn),
        logger: Some(Arc::clone(&logger)),
        shutdown_timeout: Duration::from_secs(3),
        health_check_func: Some(Arc::clone(&health_check_func)),
        health_check_interval: Duration::from_secs(4),
        delayed_task_check_interval: Duration::from_secs(5),
        group_grace_period: Duration::from_secs(6),
        group_max_delay: Duration::from_secs(7),
        group_max_size: 8,
        group_aggregator: Some(Arc::clone(&group_aggregator)),
        janitor_interval: Duration::from_secs(9),
        janitor_batch_size: 10,
    };

    assert_eq!(config.concurrency(), 4);
    assert!(Arc::ptr_eq(
        config.processing_scope().unwrap(),
        &processing_scope
    ));
    assert_eq!(config.queues().get("default"), Some(&3));
    assert_eq!(config.queues().get("critical"), Some(&6));
    assert!(config.strict_priority());
    assert_eq!(config.task_check_interval(), Duration::from_secs(2));
    assert!(Arc::ptr_eq(
        &config.retry_delay_func().unwrap().0,
        &retry_delay_func.0
    ));
    assert!(Arc::ptr_eq(&config.is_failure().unwrap().0, &is_failure.0));
    assert!(Arc::ptr_eq(
        &config.error_handler().unwrap().0,
        &error_handler.0
    ));
    assert_eq!(config.log_level(), Some(LogLevel::Warn));
    assert!(Arc::ptr_eq(config.logger().unwrap(), &logger));
    assert!(Arc::ptr_eq(
        config.health_check_func().unwrap(),
        &health_check_func
    ));
    assert_eq!(config.shutdown_timeout(), Duration::from_secs(3));
    assert_eq!(config.health_check_interval(), Duration::from_secs(4));
    assert_eq!(config.delayed_task_check_interval(), Duration::from_secs(5));
    assert_eq!(config.group_grace_period(), Duration::from_secs(6));
    assert_eq!(config.group_max_delay(), Duration::from_secs(7));
    assert_eq!(config.group_max_size(), 8);
    assert!(Arc::ptr_eq(
        config.group_aggregator().unwrap(),
        &group_aggregator
    ));
    assert_eq!(config.janitor_interval(), Duration::from_secs(9));
    assert_eq!(config.janitor_batch_size(), 10);
}

#[test]
fn server_queue_and_aggregation_models_expose_asynq_field_accessors() {
    let queue = QueueConfig::new("critical", 6);
    let aggregation =
        ServerAggregationConfig::new(Duration::from_secs(30), Duration::from_secs(300), 16);

    assert_eq!(queue.name(), "critical");
    assert_eq!(queue.priority(), 6);
    assert_eq!(aggregation.group_grace_period(), Duration::from_secs(30));
    assert_eq!(aggregation.group_max_delay(), Duration::from_secs(300));
    assert_eq!(aggregation.group_max_size(), 16);
}
