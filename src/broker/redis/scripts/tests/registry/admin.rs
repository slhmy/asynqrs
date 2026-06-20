use super::*;

#[test]
fn queue_stats_and_listing_scripts_have_sources_and_shapes() {
    let source = assert_script_shape(RedisScript::CurrentQueueStats, "current_queue_stats", 12, 2);
    assert!(source.contains("oldest_pending_since"));
    assert!(source.contains("aggregating_count"));

    let source = assert_script_shape(RedisScript::QueueMemoryUsage, "queue_memory_usage", 7, 4);
    assert!(source.contains("MEMORY"));
    assert!(source.contains("USAGE"));
    assert!(source.contains("local avg = agg_task_sample_total / agg_task_sample_size"));
    assert!(!source.contains("if agg_task_sample_size > 0"));

    let source = assert_script_shape(
        RedisScript::HistoricalQueueStats,
        "historical_queue_stats",
        0,
        0,
    );
    assert!(source.contains("GET"));
    assert!(source.contains("tonumber"));

    let source = assert_script_shape(RedisScript::GroupStats, "group_stats", 1, 1);
    assert!(source.contains("SMEMBERS"));
    assert!(source.contains("ZCARD"));

    let source = assert_script_shape(RedisScript::TaskInfo, "task_info", 1, 3);
    assert!(source.contains("HMGET"));
    assert!(source.contains("ZSCORE"));
    assert!(source.contains("NOT FOUND"));

    let source = assert_script_shape(RedisScript::ListTasks, "list_tasks", 1, 4);
    assert!(source.contains("LRANGE"));
    assert!(source.contains("ZRANGE"));
    assert!(source.contains("HMGET"));
    assert!(!source.contains("table.getn(ids), 1, -1"));
}

#[test]
fn queue_and_single_task_admin_scripts_have_sources_and_shapes() {
    let source = assert_script_shape(RedisScript::DeleteQueue, "delete_queue", 6, 1);
    assert!(source.contains("table.getn(ids) > 0"));
    assert!(source.contains("return -1"));
    assert!(source.contains("DEL"));
    assert!(!source.contains("SREM"));

    let source = assert_script_shape(RedisScript::DeleteQueueForce, "delete_queue_force", 6, 1);
    assert!(source.contains("LLEN"));
    assert!(source.contains("return -2"));
    assert!(source.contains("ARGV[1]"));
    assert!(source.contains("DEL"));
    assert!(!source.contains("SREM"));

    let source = assert_script_shape(RedisScript::DeleteTask, "delete_task", 2, 3);
    assert!(source.contains("HMGET"));
    assert!(source.contains("unique_key"));
    assert!(source.contains("state == \"active\""));

    let source = assert_script_shape(RedisScript::RunTask, "run_task", 3, 3);
    assert!(source.contains("LPUSH"));
    assert!(source.contains("state == \"pending\""));
    assert!(source.contains("\"state\", \"pending\""));

    let source = assert_script_shape(RedisScript::ArchiveTask, "archive_task", 3, 6);
    assert!(source.contains("ZADD"));
    assert!(source.contains("state == \"archived\""));
    assert!(source.contains("ZREMRANGEBYRANK"));

    let source = assert_script_shape(RedisScript::UpdateTaskPayload, "update_task_payload", 1, 1);
    assert!(source.contains("HMGET"));
    assert!(source.contains("\"scheduled\""));
    assert!(source.contains("pending_since"));
    assert!(source.contains("group"));
    assert!(source.contains("unique_key"));
    assert!(source.contains("HSET"));
}

#[test]
fn bulk_task_admin_scripts_have_sources_and_shapes() {
    let source = assert_script_shape(RedisScript::RunAllTasks, "run_all_tasks", 2, 1);
    assert!(source.contains("ZRANGE"));
    assert!(source.contains("LPUSH"));
    assert!(source.contains("\"state\", \"pending\""));

    let source = assert_script_shape(RedisScript::ArchiveAllTasks, "archive_all_tasks", 2, 4);
    assert!(source.contains("ZRANGE"));
    assert!(source.contains("ZADD"));
    assert!(source.contains("\"state\", \"archived\""));

    let source = assert_script_shape(
        RedisScript::ArchiveAllPendingTasks,
        "archive_all_pending_tasks",
        2,
        4,
    );
    assert!(source.contains("LRANGE"));
    assert!(source.contains("ZADD"));
    assert!(source.contains("\"state\", \"archived\""));

    let source = assert_script_shape(RedisScript::DeleteAllTasks, "delete_all_tasks", 1, 1);
    assert!(source.contains("ZRANGE"));
    assert!(source.contains("HGET"));
    assert!(source.contains("unique_key"));
    assert!(source.contains("DEL"));

    let source = assert_script_shape(
        RedisScript::DeleteAllPendingTasks,
        "delete_all_pending_tasks",
        1,
        1,
    );
    assert!(source.contains("LRANGE"));
    assert!(source.contains("DEL"));
    // Reference: Asynq v0.26.0 `deleteAllPendingCmd` deletes pending task
    // hashes directly and does not release unique locks, unlike `deleteAllCmd`
    // for zset-backed states.
    assert!(!source.contains("unique_key"));
}

#[test]
fn aggregating_task_admin_scripts_have_sources_and_shapes() {
    let source = assert_script_shape(
        RedisScript::RunAllAggregatingTasks,
        "run_all_aggregating_tasks",
        3,
        2,
    );
    assert!(source.contains("ZRANGE"));
    assert!(source.contains("LPUSH"));
    assert!(source.contains("\"state\", \"pending\""));

    let source = assert_script_shape(
        RedisScript::ArchiveAllAggregatingTasks,
        "archive_all_aggregating_tasks",
        3,
        5,
    );
    assert!(source.contains("ZADD"));
    assert!(source.contains("ZREMRANGEBYSCORE"));
    assert!(source.contains("\"state\", \"archived\""));

    let source = assert_script_shape(
        RedisScript::DeleteAllAggregatingTasks,
        "delete_all_aggregating_tasks",
        2,
        2,
    );
    assert!(source.contains("ZRANGE"));
    assert!(source.contains("SREM"));
    assert!(source.contains("DEL"));
    // Reference: Asynq v0.26.0 `deleteAllAggregatingCmd` deletes grouped task
    // hashes directly and does not release unique locks.
    assert!(!source.contains("unique_key"));
}
