use super::*;

#[tokio::test]
async fn async_broker_writes_server_state() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));
    let worker_a = pb::asynq::WorkerInfo {
        host: "host".to_owned(),
        pid: 123,
        server_id: "server-id".to_owned(),
        task_id: "worker-a".to_owned(),
        task_type: "email:welcome".to_owned(),
        task_payload: b"payload-a".to_vec(),
        queue: "critical".to_owned(),
        start_time: None,
        deadline: None,
    }
    .encode_to_vec();
    let worker_b = pb::asynq::WorkerInfo {
        host: "host".to_owned(),
        pid: 123,
        server_id: "server-id".to_owned(),
        task_id: "worker-b".to_owned(),
        task_type: "email:receipt".to_owned(),
        task_payload: b"payload-b".to_vec(),
        queue: "default".to_owned(),
        start_time: None,
        deadline: None,
    }
    .encode_to_vec();

    broker
        .write_server_state(
            "host",
            123,
            "server-id",
            b"server-info".to_vec(),
            [worker_a.clone(), worker_b.clone(), b"malformed".to_vec()],
            Duration::from_secs(10),
        )
        .await
        .unwrap();

    assert_eq!(
        broker.executor().calls[0],
        ExecutorCall::Zadd {
            key: "asynq:servers".to_owned(),
            score: 1_700_000_010,
            member: b"asynq:servers:{host:123:server-id}".to_vec(),
        }
    );
    assert_eq!(
        broker.executor().calls[1],
        ExecutorCall::Zadd {
            key: "asynq:workers".to_owned(),
            score: 1_700_000_010,
            member: b"asynq:workers:{host:123:server-id}".to_vec(),
        }
    );
    let ExecutorCall::EvalScriptStatus { script, keys, args } = &broker.executor().calls[2] else {
        panic!("expected write server state script call");
    };
    assert_eq!(*script, RedisScript::WriteServerState);
    assert_eq!(
        keys,
        &[
            "asynq:servers:{host:123:server-id}".to_owned(),
            "asynq:workers:{host:123:server-id}".to_owned(),
        ]
    );
    assert_eq!(
        args,
        &[
            RedisArg::I64(10),
            RedisArg::Bytes(b"server-info".to_vec()),
            RedisArg::String("worker-a".to_owned()),
            RedisArg::Bytes(worker_a),
            RedisArg::String("worker-b".to_owned()),
            RedisArg::Bytes(worker_b),
        ]
    );
}

#[tokio::test]
async fn async_broker_clears_server_state() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker
        .clear_server_state("host", 123, "server-id")
        .await
        .unwrap();

    assert_eq!(
        broker.executor().calls,
        [
            ExecutorCall::Zrem {
                key: "asynq:servers".to_owned(),
                member: "asynq:servers:{host:123:server-id}".to_owned(),
            },
            ExecutorCall::Zrem {
                key: "asynq:workers".to_owned(),
                member: "asynq:workers:{host:123:server-id}".to_owned(),
            },
            ExecutorCall::EvalScriptStatus {
                script: RedisScript::ClearServerState,
                keys: vec![
                    "asynq:servers:{host:123:server-id}".to_owned(),
                    "asynq:workers:{host:123:server-id}".to_owned(),
                ],
                args: Vec::new(),
            },
        ]
    );
}
