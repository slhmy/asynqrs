use super::*;

#[test]
fn redis_metadata_write_plans_match_script_shapes() {
    RedisWriteServerStatePlan::from_server(
        "host",
        123,
        "server-id",
        b"server-info".to_vec(),
        [b"worker-a".to_vec()],
        UNIX_EPOCH,
        Duration::from_secs(10),
    )
    .unwrap()
    .call()
    .validate()
    .unwrap();
    RedisWriteSchedulerEntriesPlan::from_entries(
        "scheduler-id",
        [("entry-a".to_owned(), b"entry-a-data".to_vec())],
        UNIX_EPOCH,
        Duration::from_secs(10),
    )
    .unwrap()
    .call()
    .validate()
    .unwrap();
}
