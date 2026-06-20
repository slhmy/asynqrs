use super::*;
use std::collections::HashMap;

#[tokio::test]
async fn async_broker_lists_servers_with_active_workers() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_byte_vec_results: vec![
            vec![
                b"asynq:servers:{host:123:server-z}".to_vec(),
                b"asynq:servers:{host:124:server-a}".to_vec(),
                b"asynq:servers:{stale:456:server-id}".to_vec(),
            ],
            vec![b"asynq:workers:{host:123:server-id}".to_vec()],
        ],
        get_bytes_results: vec![
            Some(
                pb::asynq::ServerInfo {
                    host: "host".to_owned(),
                    pid: 123,
                    server_id: "server-z".to_owned(),
                    concurrency: 2,
                    queues: HashMap::from([("critical".to_owned(), 6)]),
                    strict_priority: true,
                    status: "active".to_owned(),
                    start_time: Some(prost_types::Timestamp {
                        seconds: 1_700_000_001,
                        nanos: -1,
                    }),
                    active_worker_count: 1,
                }
                .encode_to_vec(),
            ),
            Some(
                pb::asynq::ServerInfo {
                    host: "host".to_owned(),
                    pid: 124,
                    server_id: "server-a".to_owned(),
                    concurrency: 1,
                    queues: HashMap::from([("default".to_owned(), 1)]),
                    strict_priority: false,
                    status: "active".to_owned(),
                    start_time: Some(prost_types::Timestamp {
                        seconds: 1_700_000_001,
                        nanos: 0,
                    }),
                    active_worker_count: 0,
                }
                .encode_to_vec(),
            ),
            Some(b"malformed".to_vec()),
        ],
        hvals_bytes_results: vec![vec![
            pb::asynq::WorkerInfo {
                host: "host".to_owned(),
                pid: 123,
                server_id: "server-z".to_owned(),
                task_id: "worker-z".to_owned(),
                task_type: "email:welcome".to_owned(),
                task_payload: b"payload".to_vec(),
                queue: "critical".to_owned(),
                start_time: Some(prost_types::Timestamp {
                    seconds: 1_700_000_002,
                    nanos: -1,
                }),
                deadline: Some(prost_types::Timestamp {
                    seconds: 1_700_000_060,
                    nanos: -1,
                }),
            }
            .encode_to_vec(),
            pb::asynq::WorkerInfo {
                host: "host".to_owned(),
                pid: 123,
                server_id: "server-z".to_owned(),
                task_id: "worker-a".to_owned(),
                task_type: String::new(),
                task_payload: Vec::new(),
                queue: "critical".to_owned(),
                start_time: None,
                deadline: None,
            }
            .encode_to_vec(),
            b"malformed".to_vec(),
        ]],
        ..Default::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let servers = broker.servers().await.unwrap();

    assert_eq!(servers.len(), 2);
    assert_eq!(servers[0].id(), "server-z");
    assert_eq!(servers[1].id(), "server-a");
    assert_eq!(servers[0].host(), "host");
    assert_eq!(servers[0].pid(), 123);
    assert_eq!(servers[0].concurrency(), 2);
    assert_eq!(servers[0].queues().get("critical"), Some(&6));
    assert!(servers[0].strict_priority());
    assert_eq!(servers[0].status(), "active");
    assert_eq!(
        servers[0].started(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_000) + Duration::from_nanos(999_999_999)
    );
    assert_eq!(servers[0].active_workers().len(), 2);
    assert_eq!(servers[0].active_workers()[0].task_id(), "worker-z");
    assert_eq!(servers[0].active_workers()[0].queue(), "critical");
    assert_eq!(
        servers[0].active_workers()[0].started(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_001) + Duration::from_nanos(999_999_999)
    );
    assert_eq!(
        servers[0].active_workers()[0].deadline(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_059) + Duration::from_nanos(999_999_999)
    );
    assert_eq!(servers[0].active_workers()[1].task_id(), "worker-a");
    assert_eq!(servers[0].active_workers()[0].task_type(), "email:welcome");
    assert_eq!(servers[0].active_workers()[0].task_payload(), b"payload");
    assert_eq!(servers[0].active_workers()[1].queue(), "critical");
    assert_eq!(
        broker.executor().calls,
        [
            ExecutorCall::EvalScriptByteVec {
                script: RedisScript::ListServerKeys,
                keys: vec!["asynq:servers".to_owned()],
                args: vec![RedisArg::I64(1_700_000_000)],
            },
            ExecutorCall::GetBytes {
                key: "asynq:servers:{host:123:server-z}".to_owned(),
            },
            ExecutorCall::GetBytes {
                key: "asynq:servers:{host:124:server-a}".to_owned(),
            },
            ExecutorCall::GetBytes {
                key: "asynq:servers:{stale:456:server-id}".to_owned(),
            },
            ExecutorCall::EvalScriptByteVec {
                script: RedisScript::ListWorkerKeys,
                keys: vec!["asynq:workers".to_owned()],
                args: vec![RedisArg::I64(1_700_000_000)],
            },
            ExecutorCall::HvalsBytes {
                key: "asynq:workers:{host:123:server-id}".to_owned(),
            },
        ]
    );
}

#[tokio::test]
async fn async_broker_lists_servers_skips_unreadable_metadata_keys() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_byte_vec_results: vec![
            vec![
                b"asynq:servers:{bad:123:server-bad}".to_vec(),
                b"asynq:servers:{host:123:server-id}".to_vec(),
            ],
            vec![
                b"asynq:workers:{bad:123:server-bad}".to_vec(),
                b"asynq:workers:{host:123:server-id}".to_vec(),
            ],
        ],
        get_bytes_result_results: vec![
            Err(RedisExecutorError::new("missing server metadata")),
            Ok(Some(
                pb::asynq::ServerInfo {
                    host: "host".to_owned(),
                    pid: 123,
                    server_id: "server-id".to_owned(),
                    concurrency: 2,
                    queues: HashMap::from([("critical".to_owned(), 6)]),
                    strict_priority: true,
                    status: "active".to_owned(),
                    start_time: Some(prost_types::Timestamp {
                        seconds: 1_700_000_001,
                        nanos: 0,
                    }),
                    active_worker_count: 1,
                }
                .encode_to_vec(),
            )),
        ],
        hvals_bytes_result_results: vec![
            Err(RedisExecutorError::new("missing worker metadata")),
            Ok(vec![
                pb::asynq::WorkerInfo {
                    host: "host".to_owned(),
                    pid: 123,
                    server_id: "server-id".to_owned(),
                    task_id: "task-id".to_owned(),
                    task_type: "email:welcome".to_owned(),
                    task_payload: b"payload".to_vec(),
                    queue: "critical".to_owned(),
                    start_time: Some(prost_types::Timestamp {
                        seconds: 1_700_000_002,
                        nanos: 0,
                    }),
                    deadline: Some(prost_types::Timestamp {
                        seconds: 1_700_000_060,
                        nanos: 0,
                    }),
                }
                .encode_to_vec(),
            ]),
        ],
        ..Default::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let servers = broker.servers().await.unwrap();

    assert_eq!(servers.len(), 1);
    assert_eq!(servers[0].id(), "server-id");
    assert_eq!(servers[0].active_workers().len(), 1);
    assert_eq!(servers[0].active_workers()[0].task_id(), "task-id");
    assert_eq!(
        broker.executor().calls,
        [
            ExecutorCall::EvalScriptByteVec {
                script: RedisScript::ListServerKeys,
                keys: vec!["asynq:servers".to_owned()],
                args: vec![RedisArg::I64(1_700_000_000)],
            },
            ExecutorCall::GetBytes {
                key: "asynq:servers:{bad:123:server-bad}".to_owned(),
            },
            ExecutorCall::GetBytes {
                key: "asynq:servers:{host:123:server-id}".to_owned(),
            },
            ExecutorCall::EvalScriptByteVec {
                script: RedisScript::ListWorkerKeys,
                keys: vec!["asynq:workers".to_owned()],
                args: vec![RedisArg::I64(1_700_000_000)],
            },
            ExecutorCall::HvalsBytes {
                key: "asynq:workers:{bad:123:server-bad}".to_owned(),
            },
            ExecutorCall::HvalsBytes {
                key: "asynq:workers:{host:123:server-id}".to_owned(),
            },
        ]
    );
}
