use super::*;

#[test]
fn server_with_config_and_aggregation_broker_installs_configured_group_aggregator() {
    let runtime = RecordingRuntime {
        results: Arc::new(Mutex::new(Vec::new())),
        queue_calls: Arc::new(Mutex::new(Vec::new())),
        maintenance_calls: Arc::new(Mutex::new(Vec::new())),
        shutdown_calls: Arc::new(Mutex::new(0)),
        sync_calls: Arc::new(Mutex::new(0)),
        runtime_state: None,
        stop_after_run_once: None,
        metadata_writes: Arc::new(Mutex::new(Vec::new())),
        metadata_clears: Arc::new(Mutex::new(Vec::new())),
    };
    let group_aggregator: SharedGroupAggregator = Arc::new(Mutex::new(NoopGroupAggregator));
    let config = Config {
        group_aggregator: Some(group_aggregator),
        ..Config::default()
    };

    let server = Server::with_config_and_aggregation_broker(
        runtime,
        config,
        RecordingSleeper::default(),
        NoopAggregationBroker,
        RecordingSleeper::default(),
    )
    .unwrap();

    assert!(server.aggregation_runner.is_some());
}

#[test]
fn server_aggregation_config_expands_to_queue_group_configs() {
    let runtime = RecordingRuntime {
        results: Arc::new(Mutex::new(Vec::new())),
        queue_calls: Arc::new(Mutex::new(Vec::new())),
        maintenance_calls: Arc::new(Mutex::new(Vec::new())),
        shutdown_calls: Arc::new(Mutex::new(0)),
        sync_calls: Arc::new(Mutex::new(0)),
        runtime_state: None,
        stop_after_run_once: None,
        metadata_writes: Arc::new(Mutex::new(Vec::new())),
        metadata_clears: Arc::new(Mutex::new(Vec::new())),
    };
    let aggregation_config =
        ServerAggregationConfig::new(Duration::from_secs(30), Duration::from_secs(300), 0);
    let server = test_support::server_with_weighted_queues(
        runtime,
        [("critical", 6), ("default", 3)],
        RecordingSleeper::default(),
    )
    .unwrap()
    .with_aggregation_config(aggregation_config);

    let configs = test_support::aggregation_group_configs(&server).unwrap();

    assert_eq!(server.aggregation_config, Some(aggregation_config));
    assert_eq!(configs.len(), 2);
    assert_eq!(configs[0].queue(), "critical");
    assert_eq!(configs[0].grace_period(), Duration::from_secs(30));
    assert_eq!(configs[0].max_delay(), Duration::from_secs(300));
    assert_eq!(configs[0].max_size(), 0);
    assert_eq!(configs[1].queue(), "default");
    assert_eq!(configs[1].grace_period(), Duration::from_secs(30));
    assert_eq!(configs[1].max_delay(), Duration::from_secs(300));
    assert_eq!(configs[1].max_size(), 0);
}

#[test]
fn server_aggregation_config_defaults_zero_grace_period() {
    let config = ServerAggregationConfig::new(Duration::ZERO, Duration::from_secs(300), 0);

    assert_eq!(
        config.group_grace_period(),
        DEFAULT_SERVER_GROUP_GRACE_PERIOD
    );
    assert_eq!(config.group_max_delay(), Duration::from_secs(300));
    assert_eq!(config.group_max_size(), 0);
}

#[test]
fn server_aggregation_config_preserves_signed_group_max_size() {
    let config = Config {
        group_max_size: -1,
        ..Config::default()
    };

    let aggregation_config = config.aggregation_config();

    assert_eq!(aggregation_config.group_max_size(), -1);
}

#[test]
#[should_panic(expected = "GroupGracePeriod cannot be less than a second")]
fn server_aggregation_config_rejects_subsecond_grace_period() {
    ServerAggregationConfig::new(Duration::from_millis(500), Duration::ZERO, 0);
}

#[test]
fn server_group_aggregator_builder_requires_aggregation_config() {
    let runtime = RecordingRuntime {
        results: Arc::new(Mutex::new(Vec::new())),
        queue_calls: Arc::new(Mutex::new(Vec::new())),
        maintenance_calls: Arc::new(Mutex::new(Vec::new())),
        shutdown_calls: Arc::new(Mutex::new(0)),
        sync_calls: Arc::new(Mutex::new(0)),
        runtime_state: None,
        stop_after_run_once: None,
        metadata_writes: Arc::new(Mutex::new(Vec::new())),
        metadata_clears: Arc::new(Mutex::new(Vec::new())),
    };
    let server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap();

    let error = server
        .with_group_aggregator(
            NoopAggregationBroker,
            NoopGroupAggregator,
            RecordingSleeper::default(),
        )
        .unwrap_err();

    assert_eq!(error, ServerError::MissingAggregationConfig);
}

#[test]
fn server_group_aggregator_builder_installs_runner() {
    let runtime = RecordingRuntime {
        results: Arc::new(Mutex::new(Vec::new())),
        queue_calls: Arc::new(Mutex::new(Vec::new())),
        maintenance_calls: Arc::new(Mutex::new(Vec::new())),
        shutdown_calls: Arc::new(Mutex::new(0)),
        sync_calls: Arc::new(Mutex::new(0)),
        runtime_state: None,
        stop_after_run_once: None,
        metadata_writes: Arc::new(Mutex::new(Vec::new())),
        metadata_clears: Arc::new(Mutex::new(Vec::new())),
    };
    let server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap()
            .with_aggregation_config(ServerAggregationConfig::new(
                Duration::from_secs(30),
                Duration::from_secs(300),
                0,
            ))
            .with_group_aggregator(
                NoopAggregationBroker,
                NoopGroupAggregator,
                RecordingSleeper::default(),
            )
            .unwrap();

    assert!(server.aggregation_runner.is_some());
}

#[test]
fn server_without_aggregation_config_has_no_group_configs() {
    let runtime = RecordingRuntime {
        results: Arc::new(Mutex::new(Vec::new())),
        queue_calls: Arc::new(Mutex::new(Vec::new())),
        maintenance_calls: Arc::new(Mutex::new(Vec::new())),
        shutdown_calls: Arc::new(Mutex::new(0)),
        sync_calls: Arc::new(Mutex::new(0)),
        runtime_state: None,
        stop_after_run_once: None,
        metadata_writes: Arc::new(Mutex::new(Vec::new())),
        metadata_clears: Arc::new(Mutex::new(Vec::new())),
    };
    let server =
        test_support::server_with_sleeper(runtime, ["critical"], RecordingSleeper::default())
            .unwrap();

    assert_eq!(server.aggregation_config, None);
    assert!(
        test_support::aggregation_group_configs(&server)
            .unwrap()
            .is_empty()
    );
}
