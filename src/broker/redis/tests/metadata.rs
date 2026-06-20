use super::*;

#[test]
fn async_metadata_primitives_write_and_clear_runtime_state() {
    let Some(mut fixture) = RedisFixture::new("async-metadata") else {
        return;
    };
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async_metadata_primitives_write_and_clear_runtime_state_inner(&mut fixture));
}

#[test]
fn async_redis_cancel_pubsub_cancels_active_task() {
    let Some(mut fixture) = RedisFixture::new("async-cancel-pubsub") else {
        return;
    };
    tokio::runtime::Runtime::new().unwrap().block_on(
        async_redis_cancel_pubsub_cancels_active_task_inner(&mut fixture),
    );
}

async fn async_redis_cancel_pubsub_cancels_active_task_inner(fixture: &mut RedisFixture) {
    let task = Task::new("email:cancel", b"payload".to_vec());
    fixture
        .enqueue_with(&task, fixture.enqueue_options("task-id").max_retries(3))
        .await;

    let broker = fixture.async_broker().await;
    let mut worker_assembly = WorkerAssembly::with_retry_delay(
        broker,
        PendingIntegrationHandler,
        |_retried, _error: &HandlerError, _task: &Task| Duration::from_secs(60),
    );
    let client = redis::Client::open(fixture.url.as_ref()).unwrap();
    let mut listener = RedisCancelListener::new(client, worker_assembly.canceller());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let listener_handle =
        tokio::spawn(async move { listener.run_until_stopped(shutdown_rx).await });
    let queue = fixture.queue().to_owned();
    let worker_handle = tokio::spawn(async move {
        run_worker_once(&mut worker_assembly, &[queue])
            .await
            .unwrap()
    });

    wait_for_cancel_subscriber(fixture).await;
    wait_for_state(fixture, "task-id", "active").await;
    let mut broker = fixture.async_broker().await;
    broker.cancel_processing("task-id").await.unwrap();

    assert!(matches!(
        worker_handle.await.unwrap(),
        WorkerRun::Retried { .. }
    ));
    wait_for_state(fixture, "task-id", "retry").await;
    shutdown_tx.send(true).unwrap();
    assert_eq!(listener_handle.await.unwrap().unwrap(), 1);
}

async fn wait_for_cancel_subscriber(fixture: &mut RedisFixture) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let subscribers: Vec<(String, usize)> = redis::cmd("PUBSUB")
            .arg("NUMSUB")
            .arg(super::super::keys::CANCEL_CHANNEL)
            .query(&mut fixture.connection)
            .unwrap();
        if subscribers
            .iter()
            .any(|(channel, count)| channel == super::super::keys::CANCEL_CHANNEL && *count > 0)
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for cancellation subscriber"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

struct PendingIntegrationHandler;

#[async_trait::async_trait]
impl Handler for PendingIntegrationHandler {
    async fn process_task(
        &mut self,
        _task: &Task,
        _context: &ProcessingContext,
    ) -> Result<(), HandlerError> {
        futures_util::future::pending::<()>().await;
        Ok(())
    }
}

async fn async_metadata_primitives_write_and_clear_runtime_state_inner(fixture: &mut RedisFixture) {
    fixture.clear_runtime_metadata("host", 123, "server-id");
    let worker_a = worker_info_bytes("host", 123, "server-id", "worker-a", fixture.queue());
    let worker_b = worker_info_bytes("host", 123, "server-id", "worker-b", fixture.queue());
    let mut broker = fixture.async_broker().await;
    broker
        .write_server_state(
            "host",
            123,
            "server-id",
            b"server-info".to_vec(),
            [worker_a, worker_b],
            Duration::from_secs(30),
        )
        .await
        .unwrap();

    let server_info: Vec<u8> = fixture
        .connection
        .get("asynq:servers:{host:123:server-id}")
        .unwrap();
    assert_eq!(server_info, b"server-info");
    let workers: Vec<Vec<u8>> = fixture
        .connection
        .hvals("asynq:workers:{host:123:server-id}")
        .unwrap();
    let workers = sorted(
        workers
            .into_iter()
            .map(|worker| {
                pb::asynq::WorkerInfo::decode(worker.as_slice())
                    .unwrap()
                    .task_id
            })
            .collect(),
    );
    assert_eq!(workers, ["worker-a", "worker-b"]);
    let all_servers: Vec<String> = fixture.connection.zrange("asynq:servers", 0, -1).unwrap();
    assert!(all_servers.contains(&"asynq:servers:{host:123:server-id}".to_owned()));
    let all_workers: Vec<String> = fixture.connection.zrange("asynq:workers", 0, -1).unwrap();
    assert!(all_workers.contains(&"asynq:workers:{host:123:server-id}".to_owned()));

    broker
        .write_scheduler_entries(
            "scheduler-id",
            [
                ("entry-a".to_owned(), b"entry-a-data".to_vec()),
                ("entry-b".to_owned(), b"entry-b-data".to_vec()),
            ],
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    let entries: Vec<Vec<u8>> = fixture
        .connection
        .lrange("asynq:schedulers:{scheduler-id}", 0, -1)
        .unwrap();
    let entries = sorted(
        entries
            .into_iter()
            .map(|entry| String::from_utf8(entry).unwrap())
            .collect(),
    );
    assert_eq!(entries, ["entry-a-data", "entry-b-data"]);
    let schedulers: Vec<String> = fixture
        .connection
        .zrange("asynq:schedulers", 0, -1)
        .unwrap();
    assert!(schedulers.contains(&"asynq:schedulers:{scheduler-id}".to_owned()));

    broker
        .record_scheduler_enqueue_event(
            "entry-a",
            b"enqueue-event".to_vec(),
            SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
        )
        .await
        .unwrap();
    let events: Vec<Vec<u8>> = fixture
        .connection
        .zrange("asynq:scheduler_history:entry-a", 0, -1)
        .unwrap();
    assert_eq!(events, [b"enqueue-event".to_vec()]);

    for i in 0..1000 {
        broker
            .record_scheduler_enqueue_event(
                "entry-a",
                format!("enqueue-event-{i}").into_bytes(),
                SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_001 + i),
            )
            .await
            .unwrap();
    }
    let event_count: usize = fixture
        .connection
        .zcard("asynq:scheduler_history:entry-a")
        .unwrap();
    assert_eq!(event_count, 1000);

    broker
        .clear_server_state("host", 123, "server-id")
        .await
        .unwrap();
    let server_exists: bool = fixture
        .connection
        .exists("asynq:servers:{host:123:server-id}")
        .unwrap();
    assert!(!server_exists);
    let workers_exist: bool = fixture
        .connection
        .exists("asynq:workers:{host:123:server-id}")
        .unwrap();
    assert!(!workers_exist);

    broker
        .clear_scheduler_entries("scheduler-id")
        .await
        .unwrap();
    let scheduler_exists: bool = fixture
        .connection
        .exists("asynq:schedulers:{scheduler-id}")
        .unwrap();
    assert!(!scheduler_exists);
    let schedulers: Vec<String> = fixture
        .connection
        .zrange("asynq:schedulers", 0, -1)
        .unwrap();
    assert!(!schedulers.contains(&"asynq:schedulers:{scheduler-id}".to_owned()));

    broker.clear_scheduler_history("entry-a").await.unwrap();
    let history_exists: bool = fixture
        .connection
        .exists("asynq:scheduler_history:entry-a")
        .unwrap();
    assert!(!history_exists);
}
