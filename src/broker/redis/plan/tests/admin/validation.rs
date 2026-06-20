use super::*;

#[test]
fn validates_admin_inputs() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

    assert_eq!(
        RedisPauseQueuePlan::from_queue(" ", now).unwrap_err(),
        RedisAdminPlanError::EmptyQueueName
    );
    assert_eq!(
        RedisUnpauseQueuePlan::from_queue(" ").unwrap_err(),
        RedisAdminPlanError::EmptyQueueName
    );
    assert_eq!(
        RedisDeleteTaskPlan::from_queue_and_task_id(" ", "task-id").unwrap_err(),
        RedisAdminPlanError::EmptyQueueName
    );
    assert_eq!(
        RedisRunTaskPlan::from_queue_and_task_id(" ", "task-id").unwrap_err(),
        RedisAdminPlanError::EmptyQueueName
    );
    assert_eq!(
        RedisArchiveTaskPlan::from_queue_task_id_and_time(" ", "task-id", now).unwrap_err(),
        RedisAdminPlanError::EmptyQueueName
    );
    let message = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    assert_eq!(
        RedisUpdateTaskPayloadPlan::from_queue_task_id_and_message(" ", "task-id", &message)
            .unwrap_err(),
        RedisAdminPlanError::EmptyQueueName
    );
    assert_eq!(
        RedisCurrentQueueStatsPlan::from_queue(" ", now).unwrap_err(),
        RedisAdminPlanError::EmptyQueueName
    );
    assert_eq!(
        RedisQueueMemoryUsagePlan::from_queue(" ").unwrap_err(),
        RedisAdminPlanError::EmptyQueueName
    );
    assert_eq!(
        RedisHistoricalQueueStatsPlan::from_queue(" ", now, 2).unwrap_err(),
        RedisAdminPlanError::EmptyQueueName
    );
    assert_eq!(
        RedisHistoricalQueueStatsPlan::from_queue("critical", now, 0).unwrap_err(),
        RedisAdminPlanError::NonPositiveDays
    );
    assert_eq!(
        RedisListAggregatingTasksPlan::from_queue_group_and_pagination(
            " ",
            "tenant-a",
            Pagination::new(0, 10).unwrap()
        )
        .unwrap_err(),
        RedisAdminPlanError::EmptyQueueName
    );
    assert_eq!(
        RedisRunAllTasksPlan::from_queue_and_state(" ", TaskState::Scheduled).unwrap_err(),
        RedisAdminPlanError::EmptyQueueName
    );
    assert_eq!(
        RedisRunAllTasksPlan::from_queue_and_state("critical", TaskState::Pending).unwrap_err(),
        RedisAdminPlanError::UnsupportedTaskRunAllState(TaskState::Pending)
    );
    assert_eq!(
        RedisArchiveAllTasksPlan::from_queue_state_and_time(" ", TaskState::Pending, now)
            .unwrap_err(),
        RedisAdminPlanError::EmptyQueueName
    );
    assert_eq!(
        RedisArchiveAllTasksPlan::from_queue_state_and_time("critical", TaskState::Archived, now)
            .unwrap_err(),
        RedisAdminPlanError::UnsupportedTaskArchiveAllState(TaskState::Archived)
    );
    assert_eq!(
        RedisDeleteAllTasksPlan::from_queue_and_state(" ", TaskState::Pending).unwrap_err(),
        RedisAdminPlanError::EmptyQueueName
    );
    assert_eq!(
        RedisDeleteAllTasksPlan::from_queue_and_state("critical", TaskState::Active).unwrap_err(),
        RedisAdminPlanError::UnsupportedTaskDeleteAllState(TaskState::Active)
    );
    assert_eq!(
        RedisRunAllAggregatingTasksPlan::from_queue_and_group(" ", "tenant-a").unwrap_err(),
        RedisAdminPlanError::EmptyQueueName
    );
    assert_eq!(
        RedisArchiveAllAggregatingTasksPlan::from_queue_group_and_time(" ", "tenant-a", now)
            .unwrap_err(),
        RedisAdminPlanError::EmptyQueueName
    );
    assert_eq!(
        RedisDeleteAllAggregatingTasksPlan::from_queue_and_group(" ", "tenant-a").unwrap_err(),
        RedisAdminPlanError::EmptyQueueName
    );
    assert_eq!(
        RedisListTasksPlan::from_queue_state_and_pagination(
            " ",
            TaskState::Pending,
            Pagination::new(0, 10).unwrap()
        )
        .unwrap_err(),
        RedisAdminPlanError::EmptyQueueName
    );
    assert_eq!(
        RedisListTasksPlan::from_queue_state_and_pagination(
            "critical",
            TaskState::Aggregating,
            Pagination::new(0, 10).unwrap()
        )
        .unwrap_err(),
        RedisAdminPlanError::UnsupportedTaskListState(TaskState::Aggregating)
    );
}
