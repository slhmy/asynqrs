use super::*;

#[test]
fn server_worker_count_defaults_and_builder_match_upstream_config() {
    let server = test_support::server_with_sleeper(
        recording_runtime(),
        ["critical"],
        RecordingSleeper::default(),
    )
    .unwrap();

    assert_eq!(server.worker_count, 1);

    let server = server.with_worker_count(0);
    assert_eq!(server.worker_count, 1);

    let server = server.with_worker_count(4);
    assert_eq!(server.worker_count, 4);
}

#[test]
fn server_config_defaults_match_upstream_zero_value_normalization() {
    let config = Config::default();
    let selector = config.queue_selector().unwrap();
    let aggregation_config = config.aggregation_config();

    assert!(config.effective_concurrency() >= 1);
    assert_eq!(selector.queue_names(), [DEFAULT_QUEUE_NAME.to_owned()]);
    assert!(!selector.is_strict_priority());
    assert_eq!(
        config.effective_task_check_interval(),
        DEFAULT_SERVER_IDLE_SLEEP
    );
    assert_eq!(config.effective_log_level(), LogLevel::Info);
    assert_eq!(
        config.effective_shutdown_timeout(),
        DEFAULT_SERVER_SHUTDOWN_TIMEOUT
    );
    assert_eq!(
        config.effective_health_check_interval(),
        DEFAULT_SERVER_HEALTH_CHECK_INTERVAL
    );
    assert_eq!(
        DEFAULT_SERVER_HEALTH_CHECK_INTERVAL,
        DEFAULT_SERVER_HEALTH_CHECK_INTERVAL
    );
    assert_eq!(
        config.effective_delayed_task_check_interval(),
        DEFAULT_SERVER_FORWARDER_INTERVAL
    );
    assert_eq!(
        DEFAULT_SERVER_FORWARDER_INTERVAL,
        DEFAULT_SERVER_FORWARDER_INTERVAL
    );
    assert_eq!(
        config.effective_group_grace_period(),
        DEFAULT_SERVER_GROUP_GRACE_PERIOD
    );
    assert_eq!(
        DEFAULT_SERVER_GROUP_GRACE_PERIOD,
        DEFAULT_SERVER_GROUP_GRACE_PERIOD
    );
    let default_queues = default_queue_config();
    assert_eq!(default_queues.get(DEFAULT_QUEUE_NAME), Some(&1));
    assert_eq!(default_queues.len(), 1);
    assert_eq!(
        selector.queue_priorities(),
        [(DEFAULT_QUEUE_NAME.to_owned(), 1)]
    );
    assert_eq!(
        aggregation_config.group_grace_period(),
        Duration::from_secs(60)
    );
    assert_eq!(aggregation_config.group_max_delay(), Duration::ZERO);
    assert_eq!(aggregation_config.group_max_size(), 0);
    assert_eq!(config.aggregation_config_if_enabled(), None);
    assert_eq!(
        config.effective_janitor_interval(),
        DEFAULT_SERVER_JANITOR_INTERVAL
    );
    assert_eq!(
        DEFAULT_SERVER_JANITOR_INTERVAL,
        DEFAULT_SERVER_JANITOR_INTERVAL
    );
    assert_eq!(
        config.effective_janitor_batch_size(),
        DEFAULT_JANITOR_BATCH_SIZE
    );
}

#[test]
fn server_config_preserves_signed_janitor_batch_size_like_upstream() {
    let config = Config {
        janitor_batch_size: -1,
        ..Config::default()
    };

    assert_eq!(config.effective_janitor_batch_size(), -1);
}

#[test]
fn server_config_treats_unspecified_log_level_as_info_like_upstream() {
    let config = Config {
        log_level: Some(LogLevel::Unspecified),
        ..Config::default()
    };

    assert_eq!(config.log_level(), Some(LogLevel::Unspecified));
    assert_eq!(config.effective_log_level(), LogLevel::Info);
}

#[test]
fn server_config_queue_selector_matches_upstream_filtering_and_strict_priority() {
    let mut queues = HashMap::new();
    queues.insert("low".to_owned(), 1);
    queues.insert("critical".to_owned(), 6);
    queues.insert(" ".to_owned(), 9);
    queues.insert("ignored".to_owned(), 0);

    let config = Config {
        queues,
        strict_priority: true,
        ..Config::default()
    };
    let selector = config.queue_selector().unwrap();

    assert!(selector.is_strict_priority());
    assert_eq!(
        selector.queue_priorities(),
        [("critical".to_owned(), 6), ("low".to_owned(), 1)]
    );
}
