use super::*;

#[tokio::test]
async fn executes_publish_then_enqueue_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", b"payload".to_vec());
    let plan = plan_with(
        &task,
        EnqueueOptions::new().queue(crate::QueueName::new("critical").unwrap()),
        now,
        "task-id",
    );
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker.enqueue(&plan).await.unwrap();

    let calls = &broker.executor().calls;
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls[0],
        ExecutorCall::Sadd {
            key: "asynq:queues".to_owned(),
            member: "critical".to_owned()
        }
    );
    let ExecutorCall::EvalScriptInt { script, keys, args } = &calls[1] else {
        panic!("expected script call");
    };
    assert_eq!(*script, RedisScript::Enqueue);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:pending".to_owned(),
        ]
    );
    assert!(matches!(args[0], RedisArg::Bytes(_)));
    assert_eq!(args[1], RedisArg::String("task-id".to_owned()));
    assert_eq!(args[2], RedisArg::I64(1_700_000_000_000_000_000));
}

#[tokio::test]
async fn async_broker_executes_publish_then_enqueue_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", b"payload".to_vec());
    let plan = plan_with(
        &task,
        EnqueueOptions::new().queue(crate::QueueName::new("critical").unwrap()),
        now,
        "task-id",
    );
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker.enqueue(&plan).await.unwrap();

    let calls = &broker.executor().calls;
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls[0],
        ExecutorCall::Sadd {
            key: "asynq:queues".to_owned(),
            member: "critical".to_owned()
        }
    );
    let ExecutorCall::EvalScriptInt { script, keys, args } = &calls[1] else {
        panic!("expected script call");
    };
    assert_eq!(*script, RedisScript::Enqueue);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:pending".to_owned(),
        ]
    );
    assert!(matches!(args[0], RedisArg::Bytes(_)));
    assert_eq!(args[1], RedisArg::String("task-id".to_owned()));
    assert_eq!(args[2], RedisArg::I64(1_700_000_000_000_000_000));
}

#[tokio::test]
async fn broker_publishes_each_queue_once() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let critical_task = Task::new("email:welcome", b"payload".to_vec());
    let low_task = Task::new("email:welcome", b"payload".to_vec());
    let critical_plan = plan_with(
        &critical_task,
        EnqueueOptions::new().queue(crate::QueueName::new("critical").unwrap()),
        now,
        "critical-1",
    );
    let critical_plan_2 = plan_with(
        &critical_task,
        EnqueueOptions::new().queue(crate::QueueName::new("critical").unwrap()),
        now,
        "critical-2",
    );
    let low_plan = plan_with(
        &low_task,
        EnqueueOptions::new().queue(crate::QueueName::new("low").unwrap()),
        now,
        "low-1",
    );
    let executor = FakeExecutor {
        script_int_results: vec![1, 1, 1],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    broker.enqueue(&critical_plan).await.unwrap();
    broker.enqueue(&critical_plan_2).await.unwrap();
    broker.enqueue(&low_plan).await.unwrap();

    let published = broker
        .executor()
        .calls
        .iter()
        .filter_map(|call| match call {
            ExecutorCall::Sadd { key, member } => Some((key.as_str(), member.as_str())),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        published,
        [("asynq:queues", "critical"), ("asynq:queues", "low")]
    );
}

#[tokio::test]
async fn async_broker_maps_task_id_conflict_result() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());
    let plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();
    let executor = FakeExecutor {
        script_int_results: vec![0],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker.enqueue(&plan).await.unwrap_err();

    assert_eq!(error, BrokerError::TaskIdConflict);
}

#[tokio::test]
async fn async_broker_maps_executor_errors() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());
    let plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();
    let executor = FakeExecutor {
        sadd_error: Some(RedisExecutorError::new("connection closed")),
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker.enqueue(&plan).await.unwrap_err();

    assert_eq!(error, BrokerError::Other("connection closed".to_owned()));
}

#[tokio::test]
async fn executes_unique_scheduled_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", b"payload".to_vec());
    let plan = plan_with(
        &task,
        EnqueueOptions::new()
            .queue(crate::QueueName::new("critical").unwrap())
            .process_in(Duration::from_secs(60))
            .unique_for(Duration::from_secs(300)),
        now,
        "task-id",
    );
    assert_eq!(plan.state(), TaskState::Scheduled);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker.enqueue(&plan).await.unwrap();

    let ExecutorCall::EvalScriptInt { script, keys, args } = &broker.executor().calls[1] else {
        panic!("expected script call");
    };
    assert_eq!(*script, RedisScript::ScheduleUnique);
    assert_eq!(
        keys,
        &[
            "asynq:{critical}:unique:email:welcome:321c3cf486ed509164edec1e1981fec8".to_owned(),
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:scheduled".to_owned(),
        ]
    );
    assert_eq!(args[0], RedisArg::String("task-id".to_owned()));
    assert_eq!(args[1], RedisArg::I64(360));
    assert_eq!(args[2], RedisArg::I64(1_700_000_060));
    assert!(matches!(args[3], RedisArg::Bytes(_)));
}

#[tokio::test]
async fn maps_unique_duplicate_result() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", b"payload".to_vec());
    let plan = plan_with(
        &task,
        EnqueueOptions::new().unique_for(Duration::from_secs(300)),
        now,
        "task-id",
    );
    let executor = FakeExecutor {
        script_int_results: vec![-1],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker.enqueue(&plan).await.unwrap_err();

    assert_eq!(error, BrokerError::DuplicateTask);
}

#[tokio::test]
async fn maps_task_id_conflict_result() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());
    let plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();
    let executor = FakeExecutor {
        script_int_results: vec![0],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker.enqueue(&plan).await.unwrap_err();

    assert_eq!(error, BrokerError::TaskIdConflict);
}

#[tokio::test]
async fn maps_executor_errors() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", Vec::new());
    let plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();
    let executor = FakeExecutor {
        sadd_error: Some(RedisExecutorError::new("connection closed")),
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let error = broker.enqueue(&plan).await.unwrap_err();

    assert_eq!(error, BrokerError::Other("connection closed".to_owned()));
}
