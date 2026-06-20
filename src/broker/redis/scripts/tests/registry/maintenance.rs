use super::*;

#[test]
fn maintenance_scripts_have_sources_and_shapes() {
    let source = assert_script_shape(RedisScript::Forward, "forward", 2, 4);
    assert!(source.contains("ZRANGEBYSCORE"));
    assert!(source.contains("table.getn"));
    assert!(source.contains("group ~= ''"));
    assert!(source.contains("ARGV[4] .. group"));
    assert!(!source.contains("KEYS[2] .. \":g:\""));

    let source = assert_script_shape(
        RedisScript::DeleteExpiredCompletedTasks,
        "delete_expired_completed_tasks",
        1,
        3,
    );
    assert!(source.contains("ZRANGEBYSCORE"));
    assert!(source.contains("ZREM"));
}
