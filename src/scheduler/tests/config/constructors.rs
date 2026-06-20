use super::*;

#[test]
fn generated_scheduler_ids_match_upstream_shape() {
    let scheduler = Scheduler::with_generated_id_and_clock(
        RecordingSchedulerBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap();

    assert_generated_scheduler_id(scheduler.scheduler_id.as_str());
}

#[test]
fn redis_backed_scheduler_from_redis_client_matches_constructor_shape() {
    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();

    assert_scheduler_future(RedisBackedScheduler::from_redis_client(
        redis_client,
        SchedulerOpts::default(),
    ));
}

#[test]
fn redis_backed_scheduler_from_redis_runtime_client_accepts_shared_runtime_boundary() {
    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();

    assert_scheduler_future(RedisBackedScheduler::from_redis_runtime_client(
        RedisRuntimeClient::direct(redis_client),
        SchedulerOpts::default(),
    ));
}

#[test]
fn scheduler_tracks_owned_and_shared_connections_like_upstream() {
    let owned = Scheduler::new_with_generated_id(RecordingSchedulerBroker::default()).unwrap();
    assert!(!owned.shared_connection);

    let shared = Scheduler::new_with_generated_id(RecordingSchedulerBroker::default())
        .unwrap()
        .with_shared_connection();
    assert!(shared.shared_connection);
}

#[test]
fn redis_backed_scheduler_connection_ownership_constructors_match_upstream_shapes() {
    let owned_client = redis::Client::open("redis://localhost:6379").unwrap();
    assert_scheduler_future(RedisBackedScheduler::from_redis_client(
        owned_client,
        SchedulerOpts::default(),
    ));

    let shared_client = redis::Client::open("redis://localhost:6379").unwrap();
    assert_scheduler_future(RedisBackedScheduler::from_redis_runtime_client(
        RedisRuntimeClient::direct(shared_client),
        SchedulerOpts::default(),
    ));

    let runtime_client = redis::Client::open("redis://localhost:6379").unwrap();
    assert_scheduler_future(RedisBackedScheduler::from_redis_runtime_client(
        RedisRuntimeClient::direct(runtime_client),
        SchedulerOpts::default(),
    ));
}

#[test]
fn scheduler_new_with_generated_id_keeps_custom_broker_constructor() {
    let scheduler = Scheduler::new_with_generated_id(RecordingSchedulerBroker::default()).unwrap();
    assert_generated_scheduler_id(scheduler.scheduler_id.as_str());
    assert_eq!(scheduler.timezone, DEFAULT_SCHEDULER_TIMEZONE);
}

#[test]
fn scheduler_with_options_applies_supported_upstream_options() {
    let logger: Arc<dyn Logger> = Arc::new(RecordingLogger::default());
    let scheduler = Scheduler::new_with_generated_id(RecordingSchedulerBroker::default())
        .map(|scheduler| {
            scheduler.with_scheduler_opts(SchedulerOpts {
                heartbeat_interval: Duration::from_secs(7),
                log_level: Some(LogLevel::Debug),
                logger: Some(Arc::clone(&logger)),
                location: Some(chrono_tz::Asia::Tokyo),
                ..SchedulerOpts::default()
            })
        })
        .unwrap();

    assert_generated_scheduler_id(scheduler.scheduler_id.as_str());
    assert_eq!(scheduler.heartbeat_interval, Duration::from_secs(7));
    assert_eq!(scheduler.metadata_ttl, Duration::from_secs(14));
    assert_eq!(scheduler.log_level(), LogLevel::Debug);
    assert!(Arc::ptr_eq(scheduler.logger().unwrap(), &logger));
    assert_eq!(scheduler.timezone, chrono_tz::Asia::Tokyo);
}

#[test]
fn redis_backed_scheduler_from_redis_client_matches_shared_constructor_shape() {
    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();

    assert_scheduler_future(RedisBackedScheduler::from_redis_runtime_client(
        RedisRuntimeClient::direct(redis_client),
        SchedulerOpts::default(),
    ));
}

#[test]
fn redis_backed_scheduler_from_direct_redis_client_keeps_direct_client_convenience() {
    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();

    assert_scheduler_future(RedisBackedScheduler::from_direct_redis_client(
        redis_client,
        SchedulerOpts::default(),
    ));
}

#[test]
fn redis_backed_scheduler_from_redis_client_accepts_redis_rs_client() {
    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();

    assert_scheduler_future(RedisBackedScheduler::from_redis_client(
        redis_client,
        SchedulerOpts::default(),
    ));
}
