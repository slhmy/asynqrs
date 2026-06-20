use super::*;

#[test]
fn sync_once_registers_provider_configs_with_scheduler() {
    let provider = RecordingProvider {
        configs: VecDeque::from([Ok(vec![PeriodicTaskConfig::new(
            "@every 1m",
            Task::new("email:welcome", b"payload".to_vec()),
            EnqueueOptions::new().queue(crate::QueueName::new("critical").unwrap()),
        )])]),
    };
    let scheduler = Scheduler::with_clock(
        "scheduler-id",
        TestBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap();
    let mut manager = PeriodicTaskManager::new(provider, scheduler);

    let run = manager.sync_once().unwrap();

    assert_eq!(run.registered(), 1);
    assert_eq!(run.unregistered(), 0);
    assert_eq!(run.unchanged(), 0);
    let entries = manager.scheduler().entries.as_slice();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].spec(), "@every 1m");
    assert_eq!(entries[0].task().type_name(), "email:welcome");
    assert_eq!(
        entries[0].options(),
        &EnqueueOptions::new().queue(crate::QueueName::new("critical").unwrap())
    );
}

#[test]
fn sync_once_unregisters_configs_missing_from_later_provider_snapshot() {
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
    .unwrap();
    let mut manager = PeriodicTaskManager::new(provider, scheduler);

    assert_eq!(manager.sync_once().unwrap().registered(), 1);
    let run = manager.sync_once().unwrap();

    assert_eq!(run.registered(), 0);
    assert_eq!(run.unregistered(), 1);
    assert_eq!(run.unchanged(), 0);
    assert!(manager.scheduler().entries.as_slice().is_empty());
}

#[test]
fn sync_once_registers_duplicate_new_configs_like_upstream_diff() {
    let config = PeriodicTaskConfig::new(
        "@every 1m",
        Task::new("email:welcome", b"payload".to_vec()),
        EnqueueOptions::new().queue(crate::QueueName::new("critical").unwrap()),
    );
    let provider = RecordingProvider {
        configs: VecDeque::from([Ok(vec![config.clone(), config.clone()]), Ok(vec![config])]),
    };
    let scheduler = Scheduler::with_clock(
        "scheduler-id",
        TestBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap();
    let mut manager = PeriodicTaskManager::new(provider, scheduler);

    assert_eq!(manager.sync_once().unwrap().registered(), 2);
    assert_eq!(manager.scheduler().entries.as_slice().len(), 2);
    let run = manager.sync_once().unwrap();

    assert_eq!(run.registered(), 0);
    assert_eq!(run.unregistered(), 0);
    assert_eq!(run.unchanged(), 1);
    assert_eq!(manager.scheduler().entries.as_slice().len(), 2);
}

#[test]
fn sync_once_treats_reordered_options_as_same_periodic_config() {
    let task = Task::new("email:welcome", b"payload".to_vec());
    let provider = RecordingProvider {
        configs: VecDeque::from([
            Ok(vec![PeriodicTaskConfig::new(
                "@every 1m",
                task.clone(),
                EnqueueOptions::new()
                    .queue(crate::QueueName::new("critical").unwrap())
                    .max_retries(3),
            )]),
            Ok(vec![PeriodicTaskConfig::new(
                "@every 1m",
                task,
                EnqueueOptions::new()
                    .max_retries(3)
                    .queue(crate::QueueName::new("critical").unwrap()),
            )]),
        ]),
    };
    let scheduler = Scheduler::with_clock(
        "scheduler-id",
        TestBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap();
    let mut manager = PeriodicTaskManager::new(provider, scheduler);

    assert_eq!(manager.sync_once().unwrap().registered(), 1);
    let run = manager.sync_once().unwrap();

    assert_eq!(run.registered(), 0);
    assert_eq!(run.unregistered(), 0);
    assert_eq!(run.unchanged(), 1);
    assert_eq!(manager.scheduler().entries.as_slice().len(), 1);
}

#[test]
fn sync_once_ignores_task_headers_when_hashing_periodic_configs_like_upstream() {
    let first = Task::new_with_headers(
        "email:welcome",
        b"payload".to_vec(),
        [("tenant".to_owned(), "a".to_owned())],
    );
    let second = Task::new_with_headers(
        "email:welcome",
        b"payload".to_vec(),
        [("tenant".to_owned(), "b".to_owned())],
    );
    let provider = RecordingProvider {
        configs: VecDeque::from([
            Ok(vec![PeriodicTaskConfig::new(
                "@every 1m",
                first,
                EnqueueOptions::new().queue(crate::QueueName::new("critical").unwrap()),
            )]),
            Ok(vec![PeriodicTaskConfig::new(
                "@every 1m",
                second,
                EnqueueOptions::new().queue(crate::QueueName::new("critical").unwrap()),
            )]),
        ]),
    };
    let scheduler = Scheduler::with_clock(
        "scheduler-id",
        TestBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap();
    let mut manager = PeriodicTaskManager::new(provider, scheduler);

    assert_eq!(manager.sync_once().unwrap().registered(), 1);
    let run = manager.sync_once().unwrap();

    assert_eq!(run.registered(), 0);
    assert_eq!(run.unregistered(), 0);
    assert_eq!(run.unchanged(), 1);
    assert_eq!(manager.scheduler().entries.as_slice().len(), 1);
}

#[test]
fn sync_once_returns_provider_errors_without_mutating_scheduler() {
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
    .unwrap();
    let mut manager = PeriodicTaskManager::new(provider, scheduler);

    let error = manager.sync_once().unwrap_err();

    assert_eq!(
        error,
        PeriodicTaskManagerError::Provider(PeriodicTaskConfigProviderError::Other(
            "database unavailable".to_owned()
        ))
    );
    assert!(manager.scheduler().entries.as_slice().is_empty());
}

#[test]
fn sync_once_returns_invalid_config_errors_without_mutating_scheduler() {
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
    .unwrap();
    let mut manager = PeriodicTaskManager::new(provider, scheduler);

    let error = manager.sync_once().unwrap_err();

    assert_eq!(
        error,
        PeriodicTaskManagerError::InvalidConfig(
            "PeriodicTaskConfig.Cronspec cannot be empty".to_owned()
        )
    );
    assert!(manager.scheduler().entries.as_slice().is_empty());
}
