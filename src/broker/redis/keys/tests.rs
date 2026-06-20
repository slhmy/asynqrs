use std::time::UNIX_EPOCH;

use super::*;

#[test]
fn builds_queue_local_keys_with_hash_tags() {
    assert_eq!(queue_key_prefix("critical"), "asynq:{critical}:");
    assert_eq!(task_key_prefix("critical"), "asynq:{critical}:t:");
    assert_eq!(
        task_key("critical", "task-id"),
        "asynq:{critical}:t:task-id"
    );
    assert_eq!(pending_key("critical"), "asynq:{critical}:pending");
    assert_eq!(active_key("critical"), "asynq:{critical}:active");
    assert_eq!(scheduled_key("critical"), "asynq:{critical}:scheduled");
    assert_eq!(retry_key("critical"), "asynq:{critical}:retry");
    assert_eq!(archived_key("critical"), "asynq:{critical}:archived");
    assert_eq!(lease_key("critical"), "asynq:{critical}:lease");
    assert_eq!(completed_key("critical"), "asynq:{critical}:completed");
    assert_eq!(paused_key("critical"), "asynq:{critical}:paused");
    assert_eq!(
        processed_total_key("critical"),
        "asynq:{critical}:processed"
    );
    assert_eq!(failed_total_key("critical"), "asynq:{critical}:failed");
    assert_eq!(
        processed_key(
            "critical",
            UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000)
        ),
        "asynq:{critical}:processed:2023-11-14"
    );
    assert_eq!(
        failed_key(
            "critical",
            UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000)
        ),
        "asynq:{critical}:failed:2023-11-14"
    );
}

#[test]
fn builds_global_keys() {
    assert_eq!(ALL_SERVERS, "asynq:servers");
    assert_eq!(ALL_WORKERS, "asynq:workers");
    assert_eq!(ALL_SCHEDULERS, "asynq:schedulers");
    assert_eq!(ALL_QUEUES, "asynq:queues");
    assert_eq!(CANCEL_CHANNEL, "asynq:cancel");
    assert_eq!(
        server_info_key("host", 123, "server-id"),
        "asynq:servers:{host:123:server-id}"
    );
    assert_eq!(
        workers_key("host", 123, "server-id"),
        "asynq:workers:{host:123:server-id}"
    );
    assert_eq!(
        scheduler_entries_key("scheduler-id"),
        "asynq:schedulers:{scheduler-id}"
    );
    assert_eq!(
        scheduler_history_key("entry-id"),
        "asynq:scheduler_history:entry-id"
    );
}

#[test]
fn builds_unique_key_from_payload_md5() {
    assert_eq!(
        unique_key("default", "email:welcome", b"payload"),
        "asynq:{default}:unique:email:welcome:321c3cf486ed509164edec1e1981fec8"
    );
    assert_eq!(
        unique_key("default", "email:welcome", b""),
        "asynq:{default}:unique:email:welcome:d41d8cd98f00b204e9800998ecf8427e"
    );
}

#[test]
fn builds_unique_key_from_optional_payload_like_go_nil_branch() {
    assert_eq!(
        super::unique::unique_key_from_optional_payload("default", "email:welcome", None),
        "asynq:{default}:unique:email:welcome:"
    );
    assert_eq!(
        super::unique::unique_key_from_optional_payload("default", "email:welcome", Some(b"")),
        "asynq:{default}:unique:email:welcome:d41d8cd98f00b204e9800998ecf8427e"
    );
}

#[test]
fn builds_group_keys() {
    assert_eq!(group_key_prefix("critical"), "asynq:{critical}:g:");
    assert_eq!(
        group_key("critical", "tenant-a"),
        "asynq:{critical}:g:tenant-a"
    );
    assert_eq!(
        aggregation_set_key("critical", "tenant-a", "set-id"),
        "asynq:{critical}:g:tenant-a:set-id"
    );
    assert_eq!(all_groups_key("critical"), "asynq:{critical}:groups");
    assert_eq!(
        all_aggregation_sets_key("critical"),
        "asynq:{critical}:aggregation_sets"
    );
}
