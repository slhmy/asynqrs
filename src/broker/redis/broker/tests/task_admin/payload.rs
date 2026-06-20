use super::*;

#[tokio::test]
async fn async_broker_updates_scheduled_task_payload() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut message = TaskMessage::from_task(&Task::new("email:welcome", b"old".to_vec()));
    message.id = "task-id".to_owned();
    message.queue = "critical".to_owned();
    let executor = FakeExecutor {
        sismember_results: vec![true, true],
        script_value_results: vec![task_info_value(
            &message,
            "scheduled",
            1_700_000_120,
            Vec::new(),
        )],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    broker
        .update_task_payload("critical", "task-id", b"updated".to_vec())
        .await
        .unwrap();

    assert!(matches!(
        &broker.executor().calls[0],
        ExecutorCall::Sismember { key, member }
            if key == "asynq:queues" && member == "critical"
    ));
    assert!(matches!(
        &broker.executor().calls[1],
        ExecutorCall::Sismember { key, member }
            if key == "asynq:queues" && member == "critical"
    ));
    assert!(matches!(
        &broker.executor().calls[2],
        ExecutorCall::EvalScriptValue { script, keys, args }
            if *script == RedisScript::TaskInfo
                && keys == &["asynq:{critical}:t:task-id".to_owned()]
                && args == &[
                    RedisArg::String("task-id".to_owned()),
                    RedisArg::I64(1_700_000_000),
                    RedisArg::String("asynq:{critical}:".to_owned()),
                ]
    ));
    let ExecutorCall::EvalScriptInt { script, keys, args } = &broker.executor().calls[3] else {
        panic!("expected update task payload script call");
    };
    assert_eq!(*script, RedisScript::UpdateTaskPayload);
    assert_eq!(keys, &["asynq:{critical}:t:task-id".to_owned()]);
    let [RedisArg::Bytes(encoded)] = args.as_slice() else {
        panic!("expected encoded task message");
    };
    let updated = TaskMessage::decode_from_slice(encoded).unwrap();
    assert_eq!(updated.id, "task-id");
    assert_eq!(updated.queue, "critical");
    assert_eq!(updated.payload, b"updated");
}

#[tokio::test]
async fn async_broker_update_task_payload_maps_script_results() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut message = TaskMessage::from_task(&Task::new("email:welcome", b"old".to_vec()));
    message.id = "task-id".to_owned();
    message.queue = "critical".to_owned();

    let executor = FakeExecutor {
        sismember_results: vec![true, true],
        script_error: Some(RedisExecutorError::new("NOT FOUND")),
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));
    let error = broker
        .update_task_payload("critical", "missing-id", b"updated".to_vec())
        .await
        .unwrap_err();
    assert_eq!(error, AdminError::TaskNotFound);

    let executor = FakeExecutor {
        sismember_results: vec![true, true],
        script_value_results: vec![task_info_value(
            &message,
            "scheduled",
            1_700_000_120,
            Vec::new(),
        )],
        script_int_results: vec![0],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));
    let error = broker
        .update_task_payload("critical", "missing-id", b"updated".to_vec())
        .await
        .unwrap_err();
    assert_eq!(error, AdminError::TaskNotFound);

    let executor = FakeExecutor {
        sismember_results: vec![true, true],
        script_value_results: vec![task_info_value(
            &message,
            "scheduled",
            1_700_000_120,
            Vec::new(),
        )],
        script_int_results: vec![-1],
        ..FakeExecutor::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));
    let error = broker
        .update_task_payload("critical", "pending-id", b"updated".to_vec())
        .await
        .unwrap_err();
    assert_eq!(error, AdminError::TaskNotScheduled);
}
