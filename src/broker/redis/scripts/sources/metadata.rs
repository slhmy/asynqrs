// Source: Asynq v0.26.0 `writeServerStateCmd`.
pub(in crate::broker::redis::scripts) const WRITE_SERVER_STATE_SOURCE: &str = r#"
redis.call("SETEX", KEYS[1], ARGV[1], ARGV[2])
redis.call("DEL", KEYS[2])
for i = 3, table.getn(ARGV)-1, 2 do
	redis.call("HSET", KEYS[2], ARGV[i], ARGV[i+1])
end
redis.call("EXPIRE", KEYS[2], ARGV[1])
return redis.status_reply("OK")
"#;
// Source: Asynq v0.26.0 `clearServerStateCmd`.
pub(in crate::broker::redis::scripts) const CLEAR_SERVER_STATE_SOURCE: &str = r#"
redis.call("DEL", KEYS[1])
redis.call("DEL", KEYS[2])
return redis.status_reply("OK")
"#;
// Source: Asynq v0.26.0 `listServerKeysCmd`.
pub(in crate::broker::redis::scripts) const LIST_SERVER_KEYS_SOURCE: &str = r#"
local now = tonumber(ARGV[1])
local keys = redis.call("ZRANGEBYSCORE", KEYS[1], now, "+inf")
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", now-1)
return keys
"#;
// Source: Asynq v0.26.0 `listWorkersCmd`.
pub(in crate::broker::redis::scripts) const LIST_WORKER_KEYS_SOURCE: &str = r#"
local now = tonumber(ARGV[1])
local keys = redis.call("ZRANGEBYSCORE", KEYS[1], now, "+inf")
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", now-1)
return keys
"#;
// Source: Asynq v0.26.0 `writeSchedulerEntriesCmd`.
pub(in crate::broker::redis::scripts) const WRITE_SCHEDULER_ENTRIES_SOURCE: &str = r#"
redis.call("DEL", KEYS[1])
for i = 2, #ARGV do
	redis.call("LPUSH", KEYS[1], ARGV[i])
end
redis.call("EXPIRE", KEYS[1], ARGV[1])
return redis.status_reply("OK")
"#;
// Source: Asynq v0.26.0 `listSchedulerKeysCmd`.
pub(in crate::broker::redis::scripts) const LIST_SCHEDULER_ENTRIES_SOURCE: &str = r#"
local now = tonumber(ARGV[1])
local keys = redis.call("ZRANGEBYSCORE", KEYS[1], now, "+inf")
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", now-1)
return keys
"#;
// Source: Asynq v0.26.0 `recordSchedulerEnqueueEventCmd`.
pub(in crate::broker::redis::scripts) const RECORD_SCHEDULER_ENQUEUE_EVENT_SOURCE: &str = r#"
redis.call("ZREMRANGEBYRANK", KEYS[1], 0, -ARGV[3])
redis.call("ZADD", KEYS[1], ARGV[1], ARGV[2])
return redis.status_reply("OK")
"#;
