use super::*;

#[test]
fn sync_interval_defaults_and_can_be_overridden() {
    let scheduler = Scheduler::with_clock(
        "scheduler-id",
        TestBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap();
    let default_manager = PeriodicTaskManager::new(
        RecordingProvider {
            configs: VecDeque::new(),
        },
        scheduler,
    );
    assert_eq!(
        default_manager.sync_interval(),
        DEFAULT_PERIODIC_TASK_MANAGER_SYNC_INTERVAL
    );
    assert_eq!(
        DEFAULT_PERIODIC_TASK_MANAGER_SYNC_INTERVAL,
        DEFAULT_PERIODIC_TASK_MANAGER_SYNC_INTERVAL
    );

    let (_provider, scheduler) = default_manager.into_parts();
    let zero_interval_manager = PeriodicTaskManager::new(
        RecordingProvider {
            configs: VecDeque::new(),
        },
        scheduler,
    )
    .with_sync_interval(Duration::ZERO);
    assert_eq!(
        zero_interval_manager.sync_interval(),
        DEFAULT_PERIODIC_TASK_MANAGER_SYNC_INTERVAL
    );

    let (_provider, scheduler) = zero_interval_manager.into_parts();
    let manager = PeriodicTaskManager::new(
        RecordingProvider {
            configs: VecDeque::new(),
        },
        scheduler,
    )
    .with_sync_interval(Duration::from_secs(30));

    assert_eq!(manager.sync_interval(), Duration::from_secs(30));
}

#[test]
fn new_periodic_task_manager_with_generated_scheduler_id_applies_scheduler_options() {
    let manager = PeriodicTaskManager::new_with_generated_scheduler_id(
        TestBroker::default(),
        RecordingProvider {
            configs: VecDeque::new(),
        },
        SchedulerOpts {
            heartbeat_interval: Duration::from_secs(7),
            log_level: Some(LogLevel::Debug),
            ..SchedulerOpts::default()
        },
    )
    .map(|manager| manager.with_sync_interval(Duration::from_secs(30)))
    .unwrap();

    assert_eq!(manager.sync_interval(), Duration::from_secs(30));
    assert_eq!(
        manager.scheduler().heartbeat_interval,
        Duration::from_secs(7)
    );
    assert_eq!(manager.scheduler().metadata_ttl, Duration::from_secs(14));
    assert_eq!(manager.scheduler().log_level(), LogLevel::Debug);
}

#[test]
fn new_periodic_task_manager_function_accepts_redis_rs_client() {
    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();

    std::mem::drop(RedisBackedPeriodicTaskManager::from_redis_client(
        redis_client,
        RecordingProvider {
            configs: VecDeque::new(),
        },
        SchedulerOpts::default(),
    ));
}

#[test]
fn redis_backed_periodic_task_manager_accepts_direct_redis_client() {
    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();

    std::mem::drop(RedisBackedPeriodicTaskManager::from_direct_redis_client(
        redis_client,
        RecordingProvider {
            configs: VecDeque::new(),
        },
        SchedulerOpts::default(),
    ));
}

#[test]
fn redis_backed_periodic_task_manager_alias_matches_constructor_return_type() {
    fn assert_future<F>(_future: F)
    where
        F: std::future::Future<
                Output = Result<
                    RedisBackedPeriodicTaskManager<RecordingProvider>,
                    PeriodicTaskManagerError,
                >,
            >,
    {
    }

    assert_future(RedisBackedPeriodicTaskManager::from_redis_client(
        redis::Client::open("redis://localhost:6379").unwrap(),
        RecordingProvider {
            configs: VecDeque::new(),
        },
        SchedulerOpts::default(),
    ));
}

#[test]
fn new_periodic_task_manager_accepts_runtime_client() {
    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();

    std::mem::drop(RedisBackedPeriodicTaskManager::from_redis_runtime_client(
        RedisRuntimeClient::direct(redis_client),
        RecordingProvider {
            configs: VecDeque::new(),
        },
        SchedulerOpts::default(),
    ));
}
