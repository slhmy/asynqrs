use super::*;

#[test]
fn plans_write_scheduler_entries_script() {
    let plan = RedisWriteSchedulerEntriesPlan::from_entries(
        "scheduler-id",
        [("entry-a".to_owned(), b"entry-a-data".to_vec())],
        UNIX_EPOCH + Duration::from_secs(1_700_000_000),
        Duration::from_secs(10),
    )
    .unwrap();
    let call = plan.call();

    assert_eq!(call.script(), RedisScript::WriteSchedulerEntries);
    assert_eq!(call.keys(), &["asynq:schedulers:{scheduler-id}".to_owned()]);
    assert_eq!(
        call.args(),
        &[RedisArg::I64(10), RedisArg::Bytes(b"entry-a-data".to_vec()),]
    );
    assert_eq!(plan.all_schedulers_key(), "asynq:schedulers");
    assert_eq!(
        plan.scheduler_entries_key(),
        "asynq:schedulers:{scheduler-id}"
    );
    assert_eq!(plan.expires_at(), 1_700_000_010);

    assert_eq!(
        RedisWriteSchedulerEntriesPlan::from_entries(
            "scheduler-id",
            [("entry-a".to_owned(), b"entry-a-data".to_vec())],
            UNIX_EPOCH + Duration::from_secs(1_700_000_000),
            too_large_go_duration(),
        )
        .unwrap_err(),
        RedisMetadataPlanError::TimeOverflow("scheduler metadata ttl duration")
    );
}

#[test]
fn plans_write_scheduler_entries_allows_empty_ids_and_payloads() {
    let plan = RedisWriteSchedulerEntriesPlan::from_entries(
        "",
        [("".to_owned(), Vec::new())],
        UNIX_EPOCH + Duration::from_secs(1_700_000_000),
        Duration::from_secs(10),
    )
    .unwrap();
    let call = plan.call();

    assert_eq!(call.keys(), &["asynq:schedulers:{}".to_owned()]);
    assert_eq!(
        call.args(),
        &[RedisArg::I64(10), RedisArg::Bytes(Vec::new())]
    );
    assert_eq!(plan.scheduler_entries_key(), "asynq:schedulers:{}");
}

#[test]
fn plans_list_scheduler_entries_script() {
    let plan =
        RedisListSchedulerEntriesPlan::from_time(UNIX_EPOCH + Duration::from_secs(1_700_000_000))
            .unwrap();
    let call = plan.call();

    assert_eq!(call.script(), RedisScript::ListSchedulerEntries);
    assert_eq!(call.keys(), &["asynq:schedulers".to_owned()]);
    assert_eq!(call.args(), &[RedisArg::I64(1_700_000_000)]);
}

#[test]
fn plans_list_scheduler_enqueue_events_command() {
    let pagination = Pagination::new(2, 10).unwrap();
    let plan =
        RedisListSchedulerEnqueueEventsPlan::from_entry_and_pagination("entry-id", pagination)
            .unwrap();

    assert_eq!(plan.history_key(), "asynq:scheduler_history:entry-id");
    assert_eq!(plan.pagination(), pagination);
}

#[test]
fn plans_list_scheduler_enqueue_events_allows_empty_entry_id() {
    let pagination = Pagination::new(0, 10).unwrap();
    let plan =
        RedisListSchedulerEnqueueEventsPlan::from_entry_and_pagination("", pagination).unwrap();

    assert_eq!(plan.history_key(), "asynq:scheduler_history:");
    assert_eq!(plan.pagination(), pagination);
}

#[test]
fn plans_clear_scheduler_entries_commands() {
    let plan = RedisClearSchedulerEntriesPlan::from_scheduler("scheduler-id").unwrap();

    assert_eq!(plan.entries_key(), "asynq:schedulers:{scheduler-id}");
    assert_eq!(plan.all_schedulers_key(), "asynq:schedulers");
}

#[test]
fn plans_clear_scheduler_entries_allows_empty_scheduler_id() {
    let plan = RedisClearSchedulerEntriesPlan::from_scheduler("").unwrap();

    assert_eq!(plan.entries_key(), "asynq:schedulers:{}");
    assert_eq!(plan.all_schedulers_key(), "asynq:schedulers");
}

#[test]
fn plans_record_scheduler_enqueue_event_command() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let plan = RedisRecordSchedulerEnqueueEventPlan::from_event("entry-id", b"event".to_vec(), now)
        .unwrap();

    assert_eq!(
        plan.call().script(),
        RedisScript::RecordSchedulerEnqueueEvent
    );
    assert_eq!(
        plan.call().keys(),
        &["asynq:scheduler_history:entry-id".to_owned()]
    );
    assert_eq!(
        plan.call().args(),
        &[
            RedisArg::I64(1_700_000_000),
            RedisArg::Bytes(b"event".to_vec()),
            RedisArg::I64(1000),
        ]
    );
}

#[test]
fn plans_record_scheduler_enqueue_event_allows_empty_entry_id() {
    let plan = RedisRecordSchedulerEnqueueEventPlan::from_event("", b"event".to_vec(), UNIX_EPOCH)
        .unwrap();

    assert_eq!(plan.call().keys(), &["asynq:scheduler_history:".to_owned()]);
}

#[test]
fn plans_record_scheduler_enqueue_event_allows_empty_event_data() {
    let plan = RedisRecordSchedulerEnqueueEventPlan::from_event("entry-id", Vec::new(), UNIX_EPOCH)
        .unwrap();

    assert_eq!(
        plan.call().args(),
        &[
            RedisArg::I64(0),
            RedisArg::Bytes(Vec::new()),
            RedisArg::I64(1000),
        ]
    );
}

#[test]
fn plans_clear_scheduler_history_command() {
    let plan = RedisClearSchedulerHistoryPlan::from_entry("entry-id").unwrap();

    assert_eq!(plan.history_key(), "asynq:scheduler_history:entry-id");
}

#[test]
fn plans_clear_scheduler_history_allows_empty_entry_id() {
    let plan = RedisClearSchedulerHistoryPlan::from_entry("").unwrap();

    assert_eq!(plan.history_key(), "asynq:scheduler_history:");
}
