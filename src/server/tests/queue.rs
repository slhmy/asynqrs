use super::*;

#[tokio::test]
async fn strict_priority_server_passes_priority_order_to_runtime() {
    let runtime = RecordingRuntime {
        results: Arc::new(Mutex::new(vec![Ok(WorkerRun::NoProcessableTask)])),
        queue_calls: Arc::new(Mutex::new(Vec::new())),
        maintenance_calls: Arc::new(Mutex::new(Vec::new())),
        shutdown_calls: Arc::new(Mutex::new(0)),
        sync_calls: Arc::new(Mutex::new(0)),
        runtime_state: None,
        stop_after_run_once: None,
        metadata_writes: Arc::new(Mutex::new(Vec::new())),
        metadata_clears: Arc::new(Mutex::new(Vec::new())),
    };
    let queue_calls = Arc::clone(&runtime.queue_calls);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut server = test_support::server_with_strict_priority_queues(
        runtime,
        [("low", 1), ("critical", 6), ("default", 3)],
        RecordingSleeper::default(),
    )
    .unwrap();

    let handle =
        tokio::spawn(
            async move { test_support::run_until_stopped(&mut server, shutdown_rx).await },
        );
    wait_until(Duration::from_millis(50), || async {
        !queue_calls.lock().await.is_empty()
    })
    .await;
    shutdown_tx.send(true).unwrap();
    handle.await.unwrap().unwrap();

    let calls = queue_calls.lock().await;
    assert_eq!(
        calls[0],
        vec![
            "critical".to_owned(),
            "default".to_owned(),
            "low".to_owned()
        ]
    );
}

#[test]
fn default_server_uses_weighted_queue_selection() {
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
    let server = test_support::server_with_sleeper(
        runtime,
        ["critical", "default"],
        RecordingSleeper::default(),
    )
    .unwrap();

    assert_eq!(
        server.queue_selector.queue_priorities(),
        [("critical".to_owned(), 1), ("default".to_owned(), 1)]
    );
    assert!(!server.queue_selector.is_strict_priority());
}

#[test]
fn redis_backed_server_builder_from_redis_client_matches_constructor_shape() {
    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();
    let builder = RedisBackedServerBuilder::from_redis_client(redis_client, Config::default());
    assert!(builder.shared_connection);
}

#[test]
fn server_new_keeps_custom_runtime_constructor() {
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
    let server = test_support::server_new(runtime, ["critical", "default"]).unwrap();

    assert_eq!(
        server.queues.as_ref(),
        ["critical".to_owned(), "default".to_owned()]
    );
    assert_eq!(server.idle_sleep, DEFAULT_SERVER_IDLE_SLEEP);
    assert_eq!(
        server.queue_selector.queue_priorities(),
        [("critical".to_owned(), 1), ("default".to_owned(), 1)]
    );
    assert!(!server.queue_selector.is_strict_priority());
}

#[test]
fn default_server_ignores_invalid_queues_and_defaults_when_empty() {
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
        test_support::server_with_sleeper(runtime, [" ", ""], RecordingSleeper::default()).unwrap();

    assert_eq!(server.queues.as_ref(), [DEFAULT_QUEUE_NAME.to_owned()]);
    assert_eq!(
        server.queue_selector.queue_priorities(),
        [(DEFAULT_QUEUE_NAME.to_owned(), 1)]
    );
}

#[tokio::test]
async fn weighted_priority_selector_returns_unique_queues() {
    let mut selector =
        QueueSelector::weighted_priority([("critical", 6), ("default", 3), ("low", 1)]).unwrap();

    for _ in 0..20 {
        let selected = selector.select();
        assert_eq!(selected.len(), 3);
        assert!(selected.contains(&"critical".to_owned()));
        assert!(selected.contains(&"default".to_owned()));
        assert!(selected.contains(&"low".to_owned()));
    }
}

#[test]
fn weighted_priority_selector_skips_expansion_for_single_queue() {
    let mut selector = QueueSelector::weighted_priority([("critical", usize::MAX)]).unwrap();

    assert_eq!(selector.select(), ["critical".to_owned()]);
}

#[test]
fn queue_selector_ignores_invalid_queue_configs_and_defaults_when_empty() {
    let weighted =
        QueueSelector::weighted_priority([(" ", 1), ("critical", 0), ("retry", -3), ("low", 2)])
            .unwrap();
    assert_eq!(weighted.queue_names(), ["low"]);
    assert_eq!(weighted.queue_priorities(), [("low".to_owned(), 2)]);

    let default_weighted = QueueSelector::weighted_priority(Vec::<(&str, usize)>::new()).unwrap();
    assert_eq!(
        default_weighted.queue_names(),
        [DEFAULT_QUEUE_NAME.to_owned()]
    );

    let strict =
        QueueSelector::strict_priority([(" ", 1), ("critical", 0), ("retry", -3)]).unwrap();
    assert_eq!(strict.queue_names(), [DEFAULT_QUEUE_NAME.to_owned()]);
}

#[test]
fn queue_selector_preserves_configured_priorities_for_metadata() {
    let weighted = QueueSelector::weighted_priority([("critical", 10), ("default", 5)]).unwrap();
    assert_eq!(
        weighted.queue_priorities(),
        [("critical".to_owned(), 10), ("default".to_owned(), 5)]
    );

    let strict = QueueSelector::strict_priority([("default", 5), ("critical", 10)]).unwrap();
    assert_eq!(
        strict.queue_priorities(),
        [("critical".to_owned(), 10), ("default".to_owned(), 5)]
    );
}
