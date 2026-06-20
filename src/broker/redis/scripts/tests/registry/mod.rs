use super::*;

mod admin;
mod aggregation;
mod core;
mod lifecycle;
mod maintenance;
mod metadata;

fn assert_script_shape(
    script: RedisScript,
    name: &str,
    key_count: usize,
    arg_count: usize,
) -> &'static str {
    let spec = script.spec();
    assert_eq!(spec.script(), script);
    assert_eq!(spec.name(), name);
    assert_eq!(spec.key_count(), key_count);
    assert_eq!(spec.arg_count(), arg_count);
    assert!(spec.source().contains("redis.call"));
    spec.source()
}
