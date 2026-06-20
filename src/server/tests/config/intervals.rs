use super::*;

#[test]
fn server_maintenance_intervals_default_to_upstream_components() {
    let server = test_support::server_with_sleeper(
        recording_runtime(),
        ["critical"],
        RecordingSleeper::default(),
    )
    .unwrap();

    assert_eq!(server.forwarder_interval, Duration::from_secs(5));
    assert_eq!(server.recoverer_interval, Duration::from_secs(60));
    assert_eq!(server.janitor_interval, Duration::from_secs(8));
    assert_eq!(server.syncer_interval, DEFAULT_SERVER_SYNCER_INTERVAL);
    assert_eq!(
        server.metadata_heartbeat_interval,
        DEFAULT_SERVER_METADATA_HEARTBEAT_INTERVAL
    );
    assert_eq!(
        server.health_check_interval,
        DEFAULT_SERVER_HEALTH_CHECK_INTERVAL
    );
    assert!(server.health_check_handler.is_none());
    assert_eq!(DEFAULT_SERVER_METADATA_TTL, Duration::from_secs(10));
}

#[test]
fn maintenance_interval_sets_all_lifecycle_component_intervals() {
    let server = test_support::server_with_sleeper(
        recording_runtime(),
        ["critical"],
        RecordingSleeper::default(),
    )
    .unwrap()
    .with_maintenance_interval(Duration::from_secs(2));

    assert_eq!(server.forwarder_interval, Duration::from_secs(2));
    assert_eq!(server.recoverer_interval, Duration::from_secs(2));
    assert_eq!(server.janitor_interval, Duration::from_secs(2));
}

#[test]
fn zero_runtime_durations_fall_back_to_upstream_defaults() {
    let server = test_support::server_with_sleeper(
        recording_runtime(),
        ["critical"],
        RecordingSleeper::default(),
    )
    .unwrap()
    .with_idle_sleep(Duration::ZERO)
    .with_shutdown_timeout(Duration::ZERO);

    assert_eq!(server.idle_sleep, DEFAULT_SERVER_IDLE_SLEEP);
    assert_eq!(server.shutdown_timeout, DEFAULT_SERVER_SHUTDOWN_TIMEOUT);
    assert_eq!(
        effective_metadata_heartbeat_interval(Duration::ZERO),
        DEFAULT_SERVER_METADATA_HEARTBEAT_INTERVAL
    );
}

#[test]
fn zero_maintenance_intervals_fall_back_to_upstream_defaults() {
    let server = test_support::server_with_sleeper(
        recording_runtime(),
        ["critical"],
        RecordingSleeper::default(),
    )
    .unwrap()
    .with_forwarder_interval(Duration::ZERO)
    .with_recoverer_interval(Duration::ZERO)
    .with_janitor_interval(Duration::ZERO)
    .with_syncer_interval(Duration::ZERO);

    assert_eq!(server.forwarder_interval, DEFAULT_SERVER_FORWARDER_INTERVAL);
    assert_eq!(server.recoverer_interval, DEFAULT_SERVER_RECOVERER_INTERVAL);
    assert_eq!(server.janitor_interval, DEFAULT_SERVER_JANITOR_INTERVAL);
    assert_eq!(server.syncer_interval, DEFAULT_SERVER_SYNCER_INTERVAL);

    let server = test_support::server_with_sleeper(
        recording_runtime(),
        ["critical"],
        RecordingSleeper::default(),
    )
    .unwrap()
    .with_maintenance_interval(Duration::ZERO);

    assert_eq!(server.forwarder_interval, DEFAULT_SERVER_FORWARDER_INTERVAL);
    assert_eq!(server.recoverer_interval, DEFAULT_SERVER_RECOVERER_INTERVAL);
    assert_eq!(server.janitor_interval, DEFAULT_SERVER_JANITOR_INTERVAL);
    assert_eq!(
        server
            .with_health_check_interval(Duration::ZERO)
            .health_check_interval,
        DEFAULT_SERVER_HEALTH_CHECK_INTERVAL
    );
}
