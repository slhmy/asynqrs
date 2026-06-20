// Source: Asynq v0.26.0 `enqueueCmd`.
pub(in crate::broker::redis::scripts) const ENQUEUE_SOURCE: &str = r#"
if redis.call("EXISTS", KEYS[1]) == 1 then
	return 0
end
redis.call("HSET", KEYS[1],
           "msg", ARGV[1],
           "state", "pending",
           "pending_since", ARGV[3])
redis.call("LPUSH", KEYS[2], ARGV[2])
return 1
"#;

// Source: Asynq v0.26.0 `enqueueUniqueCmd`.
pub(in crate::broker::redis::scripts) const ENQUEUE_UNIQUE_SOURCE: &str = r#"
local ok = redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", ARGV[2])
if not ok then
  return -1
end
if redis.call("EXISTS", KEYS[2]) == 1 then
  return 0
end
redis.call("HSET", KEYS[2],
           "msg", ARGV[3],
           "state", "pending",
           "pending_since", ARGV[4],
           "unique_key", KEYS[1])
redis.call("LPUSH", KEYS[3], ARGV[1])
return 1
"#;

// Source: Asynq v0.26.0 `scheduleCmd`.
pub(in crate::broker::redis::scripts) const SCHEDULE_SOURCE: &str = r#"
if redis.call("EXISTS", KEYS[1]) == 1 then
	return 0
end
redis.call("HSET", KEYS[1],
           "msg", ARGV[1],
           "state", "scheduled")
redis.call("ZADD", KEYS[2], ARGV[2], ARGV[3])
return 1
"#;

// Source: Asynq v0.26.0 `scheduleUniqueCmd`.
pub(in crate::broker::redis::scripts) const SCHEDULE_UNIQUE_SOURCE: &str = r#"
local ok = redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", ARGV[2])
if not ok then
  return -1
end
if redis.call("EXISTS", KEYS[2]) == 1 then
  return 0
end
redis.call("HSET", KEYS[2],
           "msg", ARGV[4],
           "state", "scheduled",
           "unique_key", KEYS[1])
redis.call("ZADD", KEYS[3], ARGV[3], ARGV[1])
return 1
"#;
