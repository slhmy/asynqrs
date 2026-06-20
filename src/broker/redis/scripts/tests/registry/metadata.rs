use super::*;

#[test]
fn metadata_scripts_have_sources_and_shapes() {
    let source = assert_script_shape(RedisScript::WriteServerState, "write_server_state", 2, 2);
    assert_eq!(
        RedisScript::WriteServerState.spec().arg_shape(),
        RedisScriptArgShape::AtLeast(2)
    );
    assert!(source.contains("HSET"));
    assert!(source.contains("SETEX"));

    let source = assert_script_shape(RedisScript::ClearServerState, "clear_server_state", 2, 0);
    assert!(source.contains("DEL"));
    assert!(source.contains("status_reply"));

    for (script, name) in [
        (RedisScript::ListServerKeys, "list_server_keys"),
        (RedisScript::ListWorkerKeys, "list_worker_keys"),
    ] {
        let source = assert_script_shape(script, name, 1, 1);
        assert!(source.contains("ZRANGEBYSCORE"));
        assert!(source.contains("ZREMRANGEBYSCORE"));
    }

    let source = assert_script_shape(
        RedisScript::WriteSchedulerEntries,
        "write_scheduler_entries",
        1,
        1,
    );
    assert_eq!(
        RedisScript::WriteSchedulerEntries.spec().arg_shape(),
        RedisScriptArgShape::AtLeast(1)
    );
    assert!(source.contains("LPUSH"));
    assert!(source.contains("EXPIRE"));
    assert!(source.contains("#ARGV"));

    let source = assert_script_shape(
        RedisScript::RecordSchedulerEnqueueEvent,
        "record_scheduler_enqueue_event",
        1,
        3,
    );
    assert!(source.contains("ZREMRANGEBYRANK"));
    assert!(source.contains("ZADD"));
    assert!(source.contains("-ARGV[3]"));
}
