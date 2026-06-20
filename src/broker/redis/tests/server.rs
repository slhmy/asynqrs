use super::*;

#[test]
fn async_server_with_redis_runtime_completes_task_and_stops() {
    let Some(mut fixture) = RedisFixture::new("async-server-complete") else {
        return;
    };
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async_server_with_redis_runtime_completes_task_and_stops_inner(&mut fixture));
}

async fn async_server_with_redis_runtime_completes_task_and_stops_inner(
    fixture: &mut RedisFixture,
) {
    let task = Task::new("email:welcome", b"payload".to_vec());

    fixture
        .enqueue_with(
            &task,
            fixture
                .enqueue_options("task-id")
                .retain_for(Duration::from_secs(300)),
        )
        .await;
    let broker = fixture.async_broker().await;
    let runtime = redis_worker_assembly(broker, |task: &Task| {
        assert_eq!(task.type_name(), "email:welcome");
        Ok::<(), HandlerError>(())
    });
    let mut server =
        Server::with_config(runtime, server_config(fixture.queue()), TokioSleeper).unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle =
        tokio::spawn(async move { run_server_until_stopped(&mut server, shutdown_rx).await });

    wait_for_state(fixture, "task-id", "completed").await;
    shutdown_tx.send(true).unwrap();
    let summary = tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(summary.processed(), 1);
    assert_eq!(summary.completed(), 1);
    let active_ids: Vec<String> = fixture
        .connection
        .lrange(fixture.active_key(), 0, -1)
        .unwrap();
    assert!(active_ids.is_empty());
    let completed_score: f64 = fixture
        .connection
        .zscore(fixture.completed_key(), "task-id")
        .unwrap();
    assert!(completed_score > 0.0);
}

#[test]
fn async_server_shutdown_requeues_in_flight_task() {
    let Some(mut fixture) = RedisFixture::new("async-server-requeue") else {
        return;
    };
    tokio::runtime::Runtime::new().unwrap().block_on(
        async_server_shutdown_requeues_in_flight_task_inner(&mut fixture),
    );
}

async fn async_server_shutdown_requeues_in_flight_task_inner(fixture: &mut RedisFixture) {
    let task = Task::new("email:shutdown", b"payload".to_vec());

    fixture
        .enqueue_with(&task, fixture.enqueue_options("shutdown-id"))
        .await;
    let broker = fixture.async_broker().await;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (started_tx, started_rx) = oneshot::channel();
    let runtime = redis_worker_assembly(
        broker,
        BlockingHandler {
            started_tx: Some(started_tx),
        },
    );
    let mut server = Server::with_config(runtime, server_config(fixture.queue()), TokioSleeper)
        .unwrap()
        .with_shutdown_timeout(Duration::from_millis(50));
    let handle =
        tokio::spawn(async move { run_server_until_stopped(&mut server, shutdown_rx).await });

    started_rx.await.unwrap();
    shutdown_tx.send(true).unwrap();
    let summary = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(summary.processed(), 0);
    assert_eq!(summary.completed(), 0);
    let stored: HashMap<String, Vec<u8>> = fixture
        .connection
        .hgetall(fixture.task_key("shutdown-id"))
        .unwrap();
    assert_eq!(string_field(&stored, "state"), "pending");
    let active_ids: Vec<String> = fixture
        .connection
        .lrange(fixture.active_key(), 0, -1)
        .unwrap();
    assert!(active_ids.is_empty());
    let pending_ids: Vec<String> = fixture
        .connection
        .lrange(fixture.pending_key(), 0, -1)
        .unwrap();
    assert_eq!(pending_ids, ["shutdown-id"]);
}

fn server_config(queue: &str) -> Config {
    Config::builder()
        .try_queue(queue.to_owned(), 1usize)
        .unwrap()
        .build()
}

async fn run_server_until_stopped<P>(
    server: &mut Server<P>,
    shutdown: watch::Receiver<bool>,
) -> Result<ServerRunSummary, ServerError>
where
    P: BorrowedWorkerFactory
        + ServerConnection
        + ServerHeartbeatStore
        + ServerLeaseExtender
        + ServerMaintenanceRunner
        + ServerClock
        + ServerShutdown
        + ServerSyncStore
        + Send,
{
    let (_stop_tx, stop) = watch::channel(false);
    server.run_until_stopped_with_stop(stop, shutdown).await
}

struct BlockingHandler {
    started_tx: Option<oneshot::Sender<()>>,
}

#[async_trait::async_trait]
impl Handler for BlockingHandler {
    async fn process_task(
        &mut self,
        task: &Task,
        _context: &ProcessingContext,
    ) -> Result<(), HandlerError> {
        assert_eq!(task.type_name(), "email:shutdown");
        if let Some(sender) = self.started_tx.take() {
            sender.send(()).unwrap();
        }
        std::future::pending::<()>().await;
        Ok(())
    }
}
