use super::*;

#[test]
fn redis_backed_client_exposes_async_enqueue_scope_boundary() {
    fn assert_async_enqueue(client: &mut RedisBackedClient, task: &Task) {
        let scope = ClientEnqueueScope::background();
        let _future = client.enqueue_scoped_async(&scope, task);
    }

    let _ = assert_async_enqueue;
}

#[tokio::test]
async fn ping_async_delegates_to_async_broker() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        FixedTaskIdGenerator("task-id"),
        FixedClock(now),
    );

    client.ping_async().await.unwrap();

    assert_eq!(client.broker.ping_calls, 1);
}

#[tokio::test]
async fn enqueue_scoped_async_matches_sync_option_path() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        FixedTaskIdGenerator("generated-id"),
        FixedClock(now),
    );
    let scope = ClientEnqueueScope::background();

    let result = client
        .enqueue_scoped_with_async(
            &scope,
            &task,
            EnqueueOptions::new()
                .queue(crate::QueueName::new("critical").unwrap())
                .task_id(crate::TaskId::new("async-context-id").unwrap()),
        )
        .await
        .unwrap();

    assert_eq!(result.id(), "async-context-id");
    assert_eq!(result.queue(), "critical");
    assert_eq!(client.broker.plans[0].message().id, "async-context-id");
    assert_eq!(client.broker.plans[0].message().queue, "critical");
}

#[tokio::test]
async fn enqueue_with_async_uses_background_context() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        FixedTaskIdGenerator("generated-id"),
        FixedClock(now),
    );

    let result = client
        .enqueue_with_async(
            &task,
            EnqueueOptions::new()
                .queue(crate::QueueName::new("critical").unwrap())
                .task_id(crate::TaskId::new("async-options-id").unwrap()),
        )
        .await
        .unwrap();

    assert_eq!(result.id(), "async-options-id");
    assert_eq!(result.queue(), "critical");
    assert_eq!(client.broker.plans[0].message().id, "async-options-id");
    assert_eq!(client.broker.plans[0].message().queue, "critical");
}

#[tokio::test]
async fn enqueue_with_async_applies_task_id() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        FixedTaskIdGenerator("generated-id"),
        FixedClock(now),
    );

    let result = client
        .enqueue_with_async(
            &task,
            EnqueueOptions::new().task_id(crate::TaskId::new("async-rust-id").unwrap()),
        )
        .await
        .unwrap();

    assert_eq!(result.id(), "async-rust-id");
    assert_eq!(client.broker.plans[0].message().id, "async-rust-id");
}

#[tokio::test]
async fn enqueue_scoped_optional_async_rejects_nil_task_like_upstream() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        FixedTaskIdGenerator("task-id"),
        FixedClock(now),
    );
    let scope = ClientEnqueueScope::background();

    let error = client
        .enqueue_scoped_optional_with_async(
            &scope,
            None,
            EnqueueOptions::new().queue(crate::QueueName::new("critical").unwrap()),
        )
        .await
        .unwrap_err();

    assert_eq!(error, ClientError::NilTask);
    assert_eq!(error.to_string(), "task cannot be nil");
    assert!(client.broker.plans.is_empty());
}

#[tokio::test]
async fn enqueue_optional_with_async_rejects_nil_before_planning_options() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        FixedTaskIdGenerator("task-id"),
        FixedClock(now),
    );

    let error = client
        .enqueue_optional_with_async(
            None,
            EnqueueOptions::new()
                .queue(crate::QueueName::new("critical").unwrap())
                .task_id(crate::TaskId::new("option-task-id").unwrap()),
        )
        .await
        .unwrap_err();

    assert_eq!(error, ClientError::NilTask);
    assert_eq!(error.to_string(), "task cannot be nil");
    assert!(client.broker.plans.is_empty());
}

#[tokio::test]
async fn enqueue_scoped_async_cancelled_scope_stops_before_broker_enqueue() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());
    let mut client = Client::with_parts(
        RecordingBroker::default(),
        FixedTaskIdGenerator("task-id"),
        FixedClock(now),
    );
    let scope = ClientEnqueueScope::cancelled();

    let error = client
        .enqueue_scoped_with_async(
            &scope,
            &task,
            EnqueueOptions::new().queue(crate::QueueName::new("critical").unwrap()),
        )
        .await
        .unwrap_err();

    assert_eq!(error, ClientError::Cancelled);
    assert!(client.broker.plans.is_empty());
}

#[tokio::test]
async fn enqueue_scoped_async_observes_cancellation_while_broker_enqueue_is_in_flight() {
    #[derive(Debug)]
    struct BlockingAsyncBroker {
        plans: Vec<EnqueuePlan>,
        started: tokio::sync::watch::Sender<bool>,
        release: tokio::sync::watch::Receiver<bool>,
    }

    #[async_trait::async_trait]
    impl AsyncBroker for BlockingAsyncBroker {
        async fn ping(&mut self) -> Result<(), BrokerError> {
            Ok(())
        }

        async fn enqueue(&mut self, plan: &EnqueuePlan) -> Result<(), BrokerError> {
            let _ = self.started.send(true);
            while !*self.release.borrow() {
                if self.release.changed().await.is_err() {
                    break;
                }
            }
            self.plans.push(plan.clone());
            Ok(())
        }
    }

    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());
    let (started_tx, mut started_rx) = tokio::sync::watch::channel(false);
    let (_release_tx, release_rx) = tokio::sync::watch::channel(false);
    let cancellation = tokio_util::sync::CancellationToken::new();
    let scope = ClientEnqueueScope::from_cancellation_token(cancellation.clone());
    let mut client = Client::with_parts(
        BlockingAsyncBroker {
            plans: Vec::new(),
            started: started_tx,
            release: release_rx,
        },
        FixedTaskIdGenerator("task-id"),
        FixedClock(now),
    );

    let run = tokio::spawn(async move {
        let result = client
            .enqueue_scoped_with_async(
                &scope,
                &task,
                EnqueueOptions::new().queue(crate::QueueName::new("critical").unwrap()),
            )
            .await;
        (result, client)
    });
    while !*started_rx.borrow() {
        started_rx.changed().await.unwrap();
    }
    cancellation.cancel();
    let (error, client) = run.await.unwrap();

    assert_eq!(error.unwrap_err(), ClientError::Cancelled);
    assert!(client.broker.plans.is_empty());
}
