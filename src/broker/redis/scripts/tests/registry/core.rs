use super::*;

#[test]
fn enqueue_dequeue_and_lease_scripts_have_sources_and_shapes() {
    let expected = [
        (RedisScript::Enqueue, "enqueue", 2, 3),
        (RedisScript::EnqueueUnique, "enqueue_unique", 3, 4),
        (RedisScript::Schedule, "schedule", 2, 3),
        (RedisScript::ScheduleUnique, "schedule_unique", 3, 4),
        (RedisScript::AddToGroup, "add_to_group", 3, 4),
        (RedisScript::AddToGroupUnique, "add_to_group_unique", 4, 5),
    ];

    for (script, name, key_count, arg_count) in expected {
        let source = assert_script_shape(script, name, key_count, arg_count);
        assert!(source.contains("return 1"));
    }

    let source = assert_script_shape(RedisScript::Dequeue, "dequeue", 4, 2);
    assert!(source.contains("return nil"));
    assert!(source.contains("HGET"));

    let source = assert_script_shape(RedisScript::ListLeaseExpired, "list_lease_expired", 1, 2);
    assert!(source.contains("ZRANGEBYSCORE"));
    assert!(source.contains("HGET"));
}
