use super::*;

#[test]
fn lifecycle_scripts_have_sources_and_shapes() {
    let expected = [
        (RedisScript::Done, "done", 5, 3),
        (RedisScript::DoneUnique, "done_unique", 6, 3),
        (RedisScript::MarkAsComplete, "mark_as_complete", 6, 5),
        (
            RedisScript::MarkAsCompleteUnique,
            "mark_as_complete_unique",
            7,
            5,
        ),
        (RedisScript::Retry, "retry", 8, 6),
        (RedisScript::Archive, "archive", 9, 7),
        (RedisScript::Requeue, "requeue", 4, 1),
    ];

    for (script, name, key_count, arg_count) in expected {
        let source = assert_script_shape(script, name, key_count, arg_count);
        assert!(source.contains("status_reply"));
        assert!(source.contains("NOT FOUND"));
        if script == RedisScript::Archive {
            assert!(source.contains("ZRANGEBYSCORE"));
            assert!(source.contains("KEYS[9]"));
        }
    }
}
