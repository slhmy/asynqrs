use super::*;

#[test]
fn plans_write_server_state_script() {
    let worker = pb::asynq::WorkerInfo {
        host: "host".to_owned(),
        pid: 123,
        server_id: "server-id".to_owned(),
        task_id: "worker-a".to_owned(),
        task_type: "email:welcome".to_owned(),
        task_payload: b"payload".to_vec(),
        queue: "critical".to_owned(),
        start_time: None,
        deadline: None,
    }
    .encode_to_vec();
    let empty_id_worker = pb::asynq::WorkerInfo {
        host: "host".to_owned(),
        pid: 123,
        server_id: "server-id".to_owned(),
        task_id: String::new(),
        task_type: String::new(),
        task_payload: Vec::new(),
        queue: "critical".to_owned(),
        start_time: None,
        deadline: None,
    }
    .encode_to_vec();
    let whitespace_id_worker = pb::asynq::WorkerInfo {
        host: "host".to_owned(),
        pid: 123,
        server_id: "server-id".to_owned(),
        task_id: " ".to_owned(),
        task_type: String::new(),
        task_payload: Vec::new(),
        queue: "critical".to_owned(),
        start_time: None,
        deadline: None,
    }
    .encode_to_vec();
    let plan = RedisWriteServerStatePlan::from_server(
        "host",
        123,
        "server-id",
        b"server-info".to_vec(),
        [
            worker.clone(),
            b"malformed".to_vec(),
            empty_id_worker.clone(),
            whitespace_id_worker.clone(),
        ],
        UNIX_EPOCH + Duration::from_secs(1_700_000_000),
        Duration::from_secs(10),
    )
    .unwrap();
    let call = plan.call();

    assert_eq!(call.script(), RedisScript::WriteServerState);
    assert_eq!(
        call.keys(),
        &[
            "asynq:servers:{host:123:server-id}".to_owned(),
            "asynq:workers:{host:123:server-id}".to_owned(),
        ]
    );
    assert_eq!(
        call.args(),
        &[
            RedisArg::I64(10),
            RedisArg::Bytes(b"server-info".to_vec()),
            RedisArg::String("worker-a".to_owned()),
            RedisArg::Bytes(worker),
            RedisArg::String(String::new()),
            RedisArg::Bytes(empty_id_worker),
            RedisArg::String(" ".to_owned()),
            RedisArg::Bytes(whitespace_id_worker),
        ]
    );
    assert_eq!(plan.all_servers_key(), "asynq:servers");
    assert_eq!(plan.all_workers_key(), "asynq:workers");
    assert_eq!(plan.expires_at(), 1_700_000_010);

    let empty_metadata = RedisWriteServerStatePlan::from_server(
        "",
        123,
        "",
        Vec::new(),
        [],
        UNIX_EPOCH + Duration::from_secs(1_700_000_000),
        Duration::from_secs(10),
    )
    .unwrap();
    assert_eq!(empty_metadata.server_info_key(), "asynq:servers:{:123:}");
    assert_eq!(empty_metadata.workers_key(), "asynq:workers:{:123:}");
    assert_eq!(
        empty_metadata.call().args(),
        &[RedisArg::I64(10), RedisArg::Bytes(Vec::new())]
    );

    assert_eq!(
        RedisWriteServerStatePlan::from_server(
            "host",
            123,
            "server-id",
            b"server-info".to_vec(),
            [],
            UNIX_EPOCH + Duration::from_secs(1_700_000_000),
            too_large_go_duration(),
        )
        .unwrap_err(),
        RedisMetadataPlanError::TimeOverflow("server metadata ttl duration")
    );
}

#[test]
fn plans_clear_server_state_commands() {
    let plan = RedisClearServerStatePlan::from_server("host", 123, "server-id").unwrap();

    assert_eq!(plan.server_info_key(), "asynq:servers:{host:123:server-id}");
    assert_eq!(plan.workers_key(), "asynq:workers:{host:123:server-id}");
    assert_eq!(plan.all_servers_key(), "asynq:servers");
    assert_eq!(plan.all_workers_key(), "asynq:workers");
    assert_eq!(plan.call().script(), RedisScript::ClearServerState);
    assert_eq!(
        plan.call().keys(),
        &[
            "asynq:servers:{host:123:server-id}".to_owned(),
            "asynq:workers:{host:123:server-id}".to_owned(),
        ]
    );
    assert!(plan.call().args().is_empty());

    let empty_metadata = RedisClearServerStatePlan::from_server("", 123, "").unwrap();
    assert_eq!(empty_metadata.server_info_key(), "asynq:servers:{:123:}");
    assert_eq!(empty_metadata.workers_key(), "asynq:workers:{:123:}");
}

#[test]
fn plans_list_server_and_worker_keys_scripts() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let servers = RedisListServerKeysPlan::from_time(now).unwrap();
    let workers = RedisListWorkerKeysPlan::from_time(now).unwrap();

    assert_eq!(servers.call().script(), RedisScript::ListServerKeys);
    assert_eq!(servers.call().keys(), &["asynq:servers".to_owned()]);
    assert_eq!(servers.call().args(), &[RedisArg::I64(1_700_000_000)]);
    assert_eq!(workers.call().script(), RedisScript::ListWorkerKeys);
    assert_eq!(workers.call().keys(), &["asynq:workers".to_owned()]);
    assert_eq!(workers.call().args(), &[RedisArg::I64(1_700_000_000)]);
}
