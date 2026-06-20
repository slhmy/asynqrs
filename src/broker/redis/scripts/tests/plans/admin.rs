use super::*;

#[test]
fn redis_delete_queue_plan_matches_script_shape() {
    RedisDeleteQueuePlan::from_queue("critical")
        .unwrap()
        .call()
        .validate()
        .unwrap();
    RedisDeleteQueuePlan::from_queue_and_force("critical", true)
        .unwrap()
        .call()
        .validate()
        .unwrap();
    RedisDeleteTaskPlan::from_queue_and_task_id("critical", "task-id")
        .unwrap()
        .call()
        .validate()
        .unwrap();
    RedisRunTaskPlan::from_queue_and_task_id("critical", "task-id")
        .unwrap()
        .call()
        .validate()
        .unwrap();
    RedisArchiveTaskPlan::from_queue_task_id_and_time(
        "critical",
        "task-id",
        UNIX_EPOCH + Duration::from_secs(1_700_000_000),
    )
    .unwrap()
    .call()
    .validate()
    .unwrap();
    let message = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    RedisUpdateTaskPayloadPlan::from_queue_task_id_and_message("critical", "task-id", &message)
        .unwrap()
        .call()
        .validate()
        .unwrap();
    RedisRunAllTasksPlan::from_queue_and_state("critical", TaskState::Scheduled)
        .unwrap()
        .call()
        .validate()
        .unwrap();
    RedisArchiveAllTasksPlan::from_queue_state_and_time(
        "critical",
        TaskState::Pending,
        UNIX_EPOCH + Duration::from_secs(1_700_000_000),
    )
    .unwrap()
    .call()
    .validate()
    .unwrap();
    RedisArchiveAllTasksPlan::from_queue_state_and_time(
        "critical",
        TaskState::Scheduled,
        UNIX_EPOCH + Duration::from_secs(1_700_000_000),
    )
    .unwrap()
    .call()
    .validate()
    .unwrap();
    RedisDeleteAllTasksPlan::from_queue_and_state("critical", TaskState::Pending)
        .unwrap()
        .call()
        .validate()
        .unwrap();
    RedisDeleteAllTasksPlan::from_queue_and_state("critical", TaskState::Scheduled)
        .unwrap()
        .call()
        .validate()
        .unwrap();
    RedisRunAllAggregatingTasksPlan::from_queue_and_group("critical", "tenant-a")
        .unwrap()
        .call()
        .validate()
        .unwrap();
    RedisArchiveAllAggregatingTasksPlan::from_queue_group_and_time(
        "critical",
        "tenant-a",
        UNIX_EPOCH + Duration::from_secs(1_700_000_000),
    )
    .unwrap()
    .call()
    .validate()
    .unwrap();
    RedisDeleteAllAggregatingTasksPlan::from_queue_and_group("critical", "tenant-a")
        .unwrap()
        .call()
        .validate()
        .unwrap();
    assert_eq!(
        DELETE_QUEUE_SOURCE
            .matches(r#"redis.call("DEL", ARGV[1] .. id)"#)
            .count(),
        2
    );
}

#[test]
fn redis_admin_stats_plans_match_script_shapes() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    RedisCurrentQueueStatsPlan::from_queue("critical", now)
        .unwrap()
        .call()
        .validate()
        .unwrap();
    RedisQueueMemoryUsagePlan::from_queue("critical")
        .unwrap()
        .call()
        .validate()
        .unwrap();
    RedisHistoricalQueueStatsPlan::from_queue("critical", now, 2)
        .unwrap()
        .call()
        .validate()
        .unwrap();
    RedisGroupStatsPlan::from_queue("critical")
        .unwrap()
        .call()
        .validate()
        .unwrap();
}
