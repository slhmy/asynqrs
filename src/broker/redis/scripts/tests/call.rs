use super::*;

#[test]
fn scripts_map_return_codes() {
    assert_eq!(
        RedisScript::Enqueue.result_for_code(1),
        Some(RedisScriptResult::Success)
    );
    assert_eq!(
        RedisScript::Enqueue.result_for_code(0),
        Some(RedisScriptResult::TaskIdConflict)
    );
    assert_eq!(RedisScript::Enqueue.result_for_code(-1), None);
    assert_eq!(
        RedisScript::EnqueueUnique.result_for_code(-1),
        Some(RedisScriptResult::DuplicateTask)
    );
    assert_eq!(RedisScript::Done.result_for_code(1), None);
    assert_eq!(RedisScript::Retry.result_for_code(1), None);
    assert_eq!(RedisScript::Archive.result_for_code(1), None);
    assert_eq!(RedisScript::Requeue.result_for_code(1), None);
    assert_eq!(RedisScript::Forward.result_for_code(1), None);
    assert_eq!(
        RedisScript::DeleteExpiredCompletedTasks.result_for_code(1),
        None
    );
    assert_eq!(RedisScript::AggregationCheck.result_for_code(1), None);
    assert_eq!(RedisScript::ReadAggregationSet.result_for_code(1), None);
    assert_eq!(RedisScript::DeleteAggregationSet.result_for_code(1), None);
    assert_eq!(
        RedisScript::ReclaimStaleAggregationSets.result_for_code(1),
        None
    );
    assert_eq!(RedisScript::WriteServerState.result_for_code(1), None);
    assert_eq!(RedisScript::WriteSchedulerEntries.result_for_code(1), None);
    assert_eq!(
        RedisScript::RecordSchedulerEnqueueEvent.result_for_code(1),
        None
    );
    assert_eq!(RedisScript::DeleteQueue.result_for_code(1), None);
    assert_eq!(RedisScript::DeleteQueueForce.result_for_code(1), None);
    assert_eq!(RedisScript::DeleteTask.result_for_code(1), None);
    assert_eq!(RedisScript::RunTask.result_for_code(1), None);
    assert_eq!(RedisScript::ArchiveTask.result_for_code(1), None);
    assert_eq!(RedisScript::UpdateTaskPayload.result_for_code(1), None);
    assert_eq!(RedisScript::RunAllTasks.result_for_code(1), None);
    assert_eq!(RedisScript::ArchiveAllTasks.result_for_code(1), None);
    assert_eq!(RedisScript::ArchiveAllPendingTasks.result_for_code(1), None);
    assert_eq!(RedisScript::DeleteAllTasks.result_for_code(1), None);
    assert_eq!(RedisScript::DeleteAllPendingTasks.result_for_code(1), None);
    assert_eq!(RedisScript::CurrentQueueStats.result_for_code(1), None);
    assert_eq!(RedisScript::QueueMemoryUsage.result_for_code(1), None);
    assert_eq!(RedisScript::HistoricalQueueStats.result_for_code(1), None);
    assert_eq!(RedisScript::GroupStats.result_for_code(1), None);
    assert_eq!(RedisScript::RunAllAggregatingTasks.result_for_code(1), None);
    assert_eq!(
        RedisScript::ArchiveAllAggregatingTasks.result_for_code(1),
        None
    );
    assert_eq!(
        RedisScript::DeleteAllAggregatingTasks.result_for_code(1),
        None
    );
    assert_eq!(RedisScript::ListLeaseExpired.result_for_code(1), None);
}

#[test]
fn validates_script_call_shape() {
    let keys = vec!["k1".to_owned(), "k2".to_owned()];
    let args = vec![
        RedisArg::Bytes(Vec::new()),
        RedisArg::String("task-id".to_owned()),
        RedisArg::I64(1),
    ];

    assert_eq!(RedisScript::Enqueue.validate_call(&keys, &args), Ok(()));
    assert_eq!(
        RedisScript::Enqueue.validate_call(&keys[0..1], &args),
        Err(RedisScriptCallError::WrongKeyCount {
            script: RedisScript::Enqueue,
            expected: 2,
            actual: 1,
        })
    );
    assert_eq!(
        RedisScript::Enqueue.validate_call(&keys, &args[0..2]),
        Err(RedisScriptCallError::WrongArgCount {
            script: RedisScript::Enqueue,
            expected: 3,
            actual: 2,
        })
    );
    assert_eq!(
        RedisScript::HistoricalQueueStats.validate_call(&keys, &[]),
        Ok(())
    );
    assert_eq!(
        RedisScript::HistoricalQueueStats.validate_call(&keys[0..1], &[]),
        Err(RedisScriptCallError::WrongKeyCount {
            script: RedisScript::HistoricalQueueStats,
            expected: 0,
            actual: 1,
        })
    );
}
