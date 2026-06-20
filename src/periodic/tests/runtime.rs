use super::*;

#[tokio::test]
async fn run_until_stopped_syncs_provider_snapshots_into_running_scheduler() {
    let config = PeriodicTaskConfig::new(
        "@every 1m",
        Task::new("email:welcome", b"payload".to_vec()),
        EnqueueOptions::new().queue(crate::QueueName::new("critical").unwrap()),
    );
    let provider = RecordingProvider {
        configs: VecDeque::from([Ok(vec![config]), Ok(Vec::new())]),
    };
    let scheduler = Scheduler::with_clock(
        "scheduler-id",
        TestBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap()
    .with_tick_interval(Duration::from_millis(1));
    let manager =
        PeriodicTaskManager::new(provider, scheduler).with_sync_interval(Duration::from_millis(1));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut sync_sleeper = ShutdownAfterSleeps {
        sleeps: 0,
        shutdown_after: 2,
        shutdown: shutdown_tx,
    };

    let run = manager
        .run_until_stopped(crate::server::TokioSleeper, &mut sync_sleeper, shutdown_rx)
        .await
        .unwrap();

    assert_eq!(run.syncs(), 2);
    assert_eq!(run.registered(), 1);
    assert_eq!(run.unregistered(), 1);
    assert_eq!(run.unchanged(), 0);
}

#[tokio::test]
async fn run_alias_matches_periodic_manager_run_method() {
    let provider = RecordingProvider {
        configs: VecDeque::from([Ok(Vec::new())]),
    };
    let logger = Arc::new(RecordingLogger::default());
    let scheduler_logger: Arc<dyn Logger> = logger.clone();
    let scheduler = Scheduler::with_clock(
        "scheduler-id",
        TestBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap()
    .with_logger(scheduler_logger)
    .with_log_level(LogLevel::Debug)
    .with_tick_interval(Duration::from_millis(1));
    let manager =
        PeriodicTaskManager::new(provider, scheduler).with_sync_interval(Duration::from_millis(1));

    let run = manager.run();

    std::mem::drop(run);
}

#[tokio::test]
async fn run_until_stopped_logs_periodic_manager_shutdown() {
    let provider = RecordingProvider {
        configs: VecDeque::from([Ok(Vec::new())]),
    };
    let logger = Arc::new(RecordingLogger::default());
    let scheduler_logger: Arc<dyn Logger> = logger.clone();
    let scheduler = Scheduler::with_clock(
        "scheduler-id",
        TestBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap()
    .with_logger(scheduler_logger)
    .with_log_level(LogLevel::Debug)
    .with_tick_interval(Duration::from_millis(1));
    let manager =
        PeriodicTaskManager::new(provider, scheduler).with_sync_interval(Duration::from_millis(1));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut sync_sleeper = ShutdownAfterSleeps {
        sleeps: 0,
        shutdown_after: 1,
        shutdown: shutdown_tx,
    };

    let run = manager
        .run_until_stopped(crate::server::TokioSleeper, &mut sync_sleeper, shutdown_rx)
        .await
        .unwrap();

    assert_eq!(run.syncs(), 1);
    assert_eq!(run.registered(), 0);
    let logs = logger.logs.lock().unwrap();
    assert!(logs.iter().any(|log| log == "Stopping syncer goroutine"));
}

#[tokio::test]
async fn run_until_stopped_returns_initial_sync_errors_before_starting_scheduler() {
    let provider = RecordingProvider {
        configs: VecDeque::from([Err(PeriodicTaskConfigProviderError::Other(
            "database unavailable".to_owned(),
        ))]),
    };
    let scheduler = Scheduler::with_clock(
        "scheduler-id",
        TestBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap()
    .with_tick_interval(Duration::from_millis(1));
    let manager =
        PeriodicTaskManager::new(provider, scheduler).with_sync_interval(Duration::from_millis(1));
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut sync_sleeper = ShutdownAfterSleeps {
        sleeps: 0,
        shutdown_after: 1,
        shutdown: watch::channel(false).0,
    };

    let error = manager
        .run_until_stopped(crate::server::TokioSleeper, &mut sync_sleeper, shutdown_rx)
        .await
        .unwrap_err();

    assert_eq!(
        error,
        PeriodicTaskManagerError::Startup(
            "initial call to GetConfigs failed: database unavailable".to_owned()
        )
    );
    assert_eq!(
        error.to_string(),
        "asynq: initial call to GetConfigs failed: database unavailable"
    );
}

#[tokio::test]
async fn run_until_stopped_returns_initial_invalid_configs_before_starting_scheduler() {
    let provider = RecordingProvider {
        configs: VecDeque::from([Ok(vec![PeriodicTaskConfig::new(
            "",
            Task::new("email:welcome", b"payload".to_vec()),
            EnqueueOptions::new(),
        )])]),
    };
    let scheduler = Scheduler::with_clock(
        "scheduler-id",
        TestBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap()
    .with_tick_interval(Duration::from_millis(1));
    let manager =
        PeriodicTaskManager::new(provider, scheduler).with_sync_interval(Duration::from_millis(1));
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut sync_sleeper = ShutdownAfterSleeps {
        sleeps: 0,
        shutdown_after: 1,
        shutdown: watch::channel(false).0,
    };

    let error = manager
        .run_until_stopped(crate::server::TokioSleeper, &mut sync_sleeper, shutdown_rx)
        .await
        .unwrap_err();

    assert_eq!(
        error,
        PeriodicTaskManagerError::Startup(
            "initial call to GetConfigs contained an invalid config: PeriodicTaskConfig.Cronspec cannot be empty".to_owned()
        )
    );
    assert_eq!(
        error.to_string(),
        "asynq: initial call to GetConfigs contained an invalid config: PeriodicTaskConfig.Cronspec cannot be empty"
    );
}

#[tokio::test]
async fn run_until_stopped_continues_after_runtime_sync_errors() {
    let config = PeriodicTaskConfig::new(
        "@every 1m",
        Task::new("email:welcome", b"payload".to_vec()),
        EnqueueOptions::new().queue(crate::QueueName::new("critical").unwrap()),
    );
    let provider = RecordingProvider {
        configs: VecDeque::from([
            Ok(Vec::new()),
            Err(PeriodicTaskConfigProviderError::Other(
                "database unavailable".to_owned(),
            )),
            Ok(vec![config]),
        ]),
    };
    let scheduler = Scheduler::with_clock(
        "scheduler-id",
        TestBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap()
    .with_tick_interval(Duration::from_millis(1));
    let manager =
        PeriodicTaskManager::new(provider, scheduler).with_sync_interval(Duration::from_millis(1));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut sync_sleeper = ShutdownAfterSleeps {
        sleeps: 0,
        shutdown_after: 3,
        shutdown: shutdown_tx,
    };

    let run = manager
        .run_until_stopped(crate::server::TokioSleeper, &mut sync_sleeper, shutdown_rx)
        .await
        .unwrap();

    assert_eq!(run.syncs(), 2);
    assert_eq!(run.registered(), 1);
}

#[tokio::test]
async fn run_until_stopped_logs_runtime_invalid_configs_and_keeps_syncing() {
    let valid = PeriodicTaskConfig::new(
        "@every 1m",
        Task::new("email:welcome", b"payload".to_vec()),
        EnqueueOptions::new().queue(crate::QueueName::new("critical").unwrap()),
    );
    let provider = RecordingProvider {
        configs: VecDeque::from([
            Ok(Vec::new()),
            Ok(vec![PeriodicTaskConfig::new(
                "",
                Task::new("email:invalid", b"payload".to_vec()),
                EnqueueOptions::new(),
            )]),
            Ok(vec![valid]),
        ]),
    };
    let logger = Arc::new(RecordingLogger::default());
    let scheduler_logger: Arc<dyn Logger> = logger.clone();
    let scheduler = Scheduler::with_clock(
        "scheduler-id",
        TestBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap()
    .with_logger(scheduler_logger)
    .with_tick_interval(Duration::from_millis(1));
    let manager =
        PeriodicTaskManager::new(provider, scheduler).with_sync_interval(Duration::from_millis(1));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut sync_sleeper = ShutdownAfterSleeps {
        sleeps: 0,
        shutdown_after: 3,
        shutdown: shutdown_tx,
    };

    let run = manager
        .run_until_stopped(crate::server::TokioSleeper, &mut sync_sleeper, shutdown_rx)
        .await
        .unwrap();

    assert_eq!(run.syncs(), 2);
    assert_eq!(run.registered(), 1);
    assert!(logger.logs.lock().unwrap().iter().any(|log| {
        log.contains("Failed to sync: GetConfigs returned an invalid config")
            && log.contains("PeriodicTaskConfig.Cronspec cannot be empty")
    }));
}

#[tokio::test]
async fn run_until_stopped_logs_register_errors_and_keeps_syncing() {
    let invalid = PeriodicTaskConfig::new(
        "not-a-cron-spec",
        Task::new("email:invalid", b"payload".to_vec()),
        EnqueueOptions::new(),
    );
    let valid = PeriodicTaskConfig::new(
        "@every 1m",
        Task::new("email:welcome", b"payload".to_vec()),
        EnqueueOptions::new().queue(crate::QueueName::new("critical").unwrap()),
    );
    let provider = RecordingProvider {
        configs: VecDeque::from([Ok(vec![invalid]), Ok(vec![valid])]),
    };
    let logger = Arc::new(RecordingLogger::default());
    let scheduler_logger: Arc<dyn Logger> = logger.clone();
    let scheduler = Scheduler::with_clock(
        "scheduler-id",
        TestBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap()
    .with_logger(scheduler_logger)
    .with_tick_interval(Duration::from_millis(1));
    let manager =
        PeriodicTaskManager::new(provider, scheduler).with_sync_interval(Duration::from_millis(1));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut sync_sleeper = ShutdownAfterSleeps {
        sleeps: 0,
        shutdown_after: 2,
        shutdown: shutdown_tx,
    };

    let run = manager
        .run_until_stopped(crate::server::TokioSleeper, &mut sync_sleeper, shutdown_rx)
        .await
        .unwrap();

    assert_eq!(run.syncs(), 2);
    assert_eq!(run.registered(), 1);
    assert!(logger.logs.lock().unwrap().iter().any(|log| {
        log.contains("Failed to register periodic task")
            && log.contains("cronspec=\"not-a-cron-spec\"")
    }));
}

#[tokio::test]
async fn start_runs_manager_in_background_until_handle_shutdown() {
    let config = PeriodicTaskConfig::new(
        "@every 1m",
        Task::new("email:welcome", b"payload".to_vec()),
        EnqueueOptions::new().queue(crate::QueueName::new("critical").unwrap()),
    );
    let provider = RecordingProvider {
        configs: VecDeque::from([Ok(vec![config]), Ok(Vec::new())]),
    };
    let logger = Arc::new(RecordingLogger::default());
    let scheduler_logger: Arc<dyn Logger> = logger.clone();
    let scheduler = Scheduler::with_clock(
        "scheduler-id",
        TestBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap()
    .with_logger(scheduler_logger)
    .with_log_level(LogLevel::Debug)
    .with_tick_interval(Duration::from_millis(1));
    let manager =
        PeriodicTaskManager::new(provider, scheduler).with_sync_interval(Duration::from_millis(1));

    let handle = manager.start().unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    let run = handle.shutdown().await.unwrap();

    assert!(run.syncs() >= 2);
    assert_eq!(run.registered(), 1);
    assert!(run.unregistered() <= 1);
    assert!(
        logger
            .logs
            .lock()
            .unwrap()
            .iter()
            .any(|log| log == "Stopping syncer goroutine")
    );
}

#[tokio::test]
async fn start_returns_initial_sync_errors_before_spawning_background_manager() {
    let provider = RecordingProvider {
        configs: VecDeque::from([Err(PeriodicTaskConfigProviderError::Other(
            "database unavailable".to_owned(),
        ))]),
    };
    let scheduler = Scheduler::with_clock(
        "scheduler-id",
        TestBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap()
    .with_tick_interval(Duration::from_millis(1));
    let manager =
        PeriodicTaskManager::new(provider, scheduler).with_sync_interval(Duration::from_millis(1));

    let error = manager.start().unwrap_err();

    assert_eq!(
        error,
        PeriodicTaskManagerError::Startup(
            "initial call to GetConfigs failed: database unavailable".to_owned()
        )
    );
    assert_eq!(
        error.to_string(),
        "asynq: initial call to GetConfigs failed: database unavailable"
    );
}

#[tokio::test]
async fn start_wraps_scheduler_start_errors_with_periodic_prefix_like_upstream() {
    let provider = RecordingProvider {
        configs: VecDeque::from([Ok(Vec::new())]),
    };
    let mut scheduler = Scheduler::with_clock(
        "scheduler-id",
        TestBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap()
    .with_tick_interval(Duration::from_millis(1));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    shutdown_tx.send(true).unwrap();
    let mut sleeper = ShutdownAfterSleeps {
        sleeps: 0,
        shutdown_after: 1,
        shutdown: watch::channel(false).0,
    };
    scheduler
        .run_until_stopped(&mut sleeper, shutdown_rx)
        .await
        .unwrap();
    let manager = PeriodicTaskManager::new(provider, scheduler);

    let error = manager.start().unwrap_err();

    assert_eq!(
        error,
        PeriodicTaskManagerError::Startup(
            "asynq: the scheduler has already been stopped".to_owned()
        )
    );
    assert_eq!(
        error.to_string(),
        "asynq: asynq: the scheduler has already been stopped"
    );
}

#[tokio::test]
async fn start_and_shutdown_aliases_match_upstream_names() {
    let provider = RecordingProvider {
        configs: VecDeque::from([Ok(Vec::new())]),
    };
    let scheduler = Scheduler::with_clock(
        "scheduler-id",
        TestBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap()
    .with_tick_interval(Duration::from_millis(1));
    let manager =
        PeriodicTaskManager::new(provider, scheduler).with_sync_interval(Duration::from_millis(1));

    let handle = manager.start().unwrap();
    let run = handle.shutdown().await.unwrap();

    assert_eq!(run.registered(), 0);
}
