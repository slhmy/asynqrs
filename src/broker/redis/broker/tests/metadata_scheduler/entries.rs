use super::*;

#[tokio::test]
async fn async_broker_writes_scheduler_entries() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker
        .write_scheduler_entries(
            "scheduler-id",
            [
                ("entry-a".to_owned(), b"entry-a-data".to_vec()),
                ("entry-b".to_owned(), b"entry-b-data".to_vec()),
            ],
            Duration::from_secs(10),
        )
        .await
        .unwrap();

    assert_eq!(
        broker.executor().calls[0],
        ExecutorCall::Zadd {
            key: "asynq:schedulers".to_owned(),
            score: 1_700_000_010,
            member: b"asynq:schedulers:{scheduler-id}".to_vec(),
        }
    );
    let ExecutorCall::EvalScriptStatus { script, keys, args } = &broker.executor().calls[1] else {
        panic!("expected write scheduler entries script call");
    };
    assert_eq!(*script, RedisScript::WriteSchedulerEntries);
    assert_eq!(keys, &["asynq:schedulers:{scheduler-id}".to_owned()]);
    assert_eq!(
        args,
        &[
            RedisArg::I64(10),
            RedisArg::Bytes(b"entry-a-data".to_vec()),
            RedisArg::Bytes(b"entry-b-data".to_vec()),
        ]
    );
}

#[tokio::test]
async fn async_broker_clears_scheduler_entries() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

    broker
        .clear_scheduler_entries("scheduler-id")
        .await
        .unwrap();

    assert_eq!(
        broker.executor().calls,
        [
            ExecutorCall::Zrem {
                key: "asynq:schedulers".to_owned(),
                member: "asynq:schedulers:{scheduler-id}".to_owned(),
            },
            ExecutorCall::Del {
                key: "asynq:schedulers:{scheduler-id}".to_owned(),
            },
        ]
    );
}

#[tokio::test]
async fn async_broker_lists_scheduler_entries() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_byte_vec_results: vec![vec![b"asynq:schedulers:{scheduler-id}".to_vec()]],
        lrange_bytes_results: vec![vec![
            pb::asynq::SchedulerEntry {
                id: "entry-b".to_owned(),
                spec: "@every 1m".to_owned(),
                task_type: "email:welcome".to_owned(),
                task_payload: b"payload".to_vec(),
                enqueue_options: vec![
                    "Queue(\"critical\")".to_owned(),
                    "MaxRetry(5)".to_owned(),
                    "malformed".to_owned(),
                ],
                next_enqueue_time: Some(prost_types::Timestamp {
                    seconds: 1_700_000_060,
                    nanos: 0,
                }),
                prev_enqueue_time: Some(prost_types::Timestamp {
                    seconds: -62_135_596_800,
                    nanos: 0,
                }),
            }
            .encode_to_vec(),
            pb::asynq::SchedulerEntry {
                id: "entry-a".to_owned(),
                spec: "@every 2m".to_owned(),
                task_type: "email:followup".to_owned(),
                task_payload: b"followup".to_vec(),
                enqueue_options: vec!["Queue(\"default\")".to_owned()],
                next_enqueue_time: Some(prost_types::Timestamp {
                    seconds: 1_700_000_120,
                    nanos: 0,
                }),
                prev_enqueue_time: Some(prost_types::Timestamp {
                    seconds: -62_135_596_800,
                    nanos: 0,
                }),
            }
            .encode_to_vec(),
        ]],
        ..Default::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let entries = broker.scheduler_entries().await.unwrap();

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].id(), "entry-b");
    assert_eq!(entries[1].id(), "entry-a");
    assert_eq!(entries[0].spec(), "@every 1m");
    assert_eq!(entries[0].task().type_name(), "email:welcome");
    assert_eq!(entries[0].task().payload(), b"payload");
    assert_eq!(
        entries[0].enqueue_options(),
        &["Queue(\"critical\")", "MaxRetry(5)", "malformed"]
    );
    assert_eq!(
        entries[0].next_enqueue_at(),
        UNIX_EPOCH + Duration::from_secs(1_700_000_060)
    );
    assert_eq!(entries[0].prev_enqueue_at(), None);
    assert_eq!(
        broker.executor().calls,
        [
            ExecutorCall::EvalScriptByteVec {
                script: RedisScript::ListSchedulerEntries,
                keys: vec!["asynq:schedulers".to_owned()],
                args: vec![RedisArg::I64(1_700_000_000)],
            },
            ExecutorCall::LrangeBytes {
                key: "asynq:schedulers:{scheduler-id}".to_owned(),
                start: 0,
                stop: -1,
            },
        ]
    );
}

#[tokio::test]
async fn async_broker_lists_scheduler_entries_skips_bad_data() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_byte_vec_results: vec![vec![
            b"asynq:schedulers:{bad-entry}".to_vec(),
            b"\xff".to_vec(),
            b"asynq:schedulers:{good-entry}".to_vec(),
        ]],
        lrange_bytes_results: vec![
            vec![b"\x80".to_vec()],
            vec![
                pb::asynq::SchedulerEntry {
                    id: "entry-id".to_owned(),
                    spec: "@every 1m".to_owned(),
                    task_type: "email:welcome".to_owned(),
                    task_payload: b"payload".to_vec(),
                    enqueue_options: Vec::new(),
                    next_enqueue_time: Some(prost_types::Timestamp {
                        seconds: 1_700_000_060,
                        nanos: 0,
                    }),
                    prev_enqueue_time: Some(prost_types::Timestamp {
                        seconds: -62_135_596_800,
                        nanos: 0,
                    }),
                }
                .encode_to_vec(),
            ],
        ],
        ..Default::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let entries = broker.scheduler_entries().await.unwrap();

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].id(), "entry-id");
    assert_eq!(
        broker.executor().calls,
        [
            ExecutorCall::EvalScriptByteVec {
                script: RedisScript::ListSchedulerEntries,
                keys: vec!["asynq:schedulers".to_owned()],
                args: vec![RedisArg::I64(1_700_000_000)],
            },
            ExecutorCall::LrangeBytes {
                key: "asynq:schedulers:{bad-entry}".to_owned(),
                start: 0,
                stop: -1,
            },
            ExecutorCall::LrangeBytes {
                key: "asynq:schedulers:{good-entry}".to_owned(),
                start: 0,
                stop: -1,
            },
        ]
    );
}

#[tokio::test]
async fn async_broker_lists_scheduler_entries_defaults_missing_timestamps() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let executor = FakeExecutor {
        script_byte_vec_results: vec![vec![b"asynq:schedulers:{scheduler-id}".to_vec()]],
        lrange_bytes_results: vec![vec![
            pb::asynq::SchedulerEntry {
                id: "entry-id".to_owned(),
                spec: "@every 1m".to_owned(),
                task_type: "email:welcome".to_owned(),
                task_payload: b"payload".to_vec(),
                enqueue_options: Vec::new(),
                next_enqueue_time: None,
                prev_enqueue_time: None,
            }
            .encode_to_vec(),
        ]],
        ..Default::default()
    };
    let mut broker = RedisBroker::with_clock(executor, TestClock(now));

    let entries = broker.scheduler_entries().await.unwrap();

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].id(), "entry-id");
    assert_eq!(entries[0].next_enqueue_at(), UNIX_EPOCH);
    assert_eq!(entries[0].prev_enqueue_at(), Some(UNIX_EPOCH));
}
