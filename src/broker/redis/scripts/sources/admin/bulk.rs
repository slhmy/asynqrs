// Source: Asynq v0.26.0 `runAllCmd`.
pub(in crate::broker::redis::scripts) const RUN_ALL_TASKS_SOURCE: &str = r#"
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
  redis.call("LPUSH", KEYS[2], id)
  redis.call("HSET", ARGV[1] .. id, "state", "pending")
end
redis.call("DEL", KEYS[1])
return table.getn(ids)
"#;

// Source: Asynq v0.26.0 `archiveAllCmd`.
pub(in crate::broker::redis::scripts) const ARCHIVE_ALL_TASKS_SOURCE: &str = r#"
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
  redis.call("ZADD", KEYS[2], ARGV[1], id)
  redis.call("HSET", ARGV[4] .. id, "state", "archived")
end
redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", ARGV[2])
redis.call("ZREMRANGEBYRANK", KEYS[2], 0, -ARGV[3])
redis.call("DEL", KEYS[1])
return table.getn(ids)
"#;

// Source: Asynq v0.26.0 `archiveAllPendingCmd`.
pub(in crate::broker::redis::scripts) const ARCHIVE_ALL_PENDING_TASKS_SOURCE: &str = r#"
local ids = redis.call("LRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
  redis.call("ZADD", KEYS[2], ARGV[1], id)
  redis.call("HSET", ARGV[4] .. id, "state", "archived")
end
redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", ARGV[2])
redis.call("ZREMRANGEBYRANK", KEYS[2], 0, -ARGV[3])
redis.call("DEL", KEYS[1])
return table.getn(ids)
"#;

// Source: Asynq v0.26.0 `deleteAllCmd`.
pub(in crate::broker::redis::scripts) const DELETE_ALL_TASKS_SOURCE: &str = r#"
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
  local task_key = ARGV[1] .. id
  local unique_key = redis.call("HGET", task_key, "unique_key")
  if unique_key and unique_key ~= "" and redis.call("GET", unique_key) == id then
    redis.call("DEL", unique_key)
  end
  redis.call("DEL", task_key)
end
redis.call("DEL", KEYS[1])
return table.getn(ids)
"#;

// Source: Asynq v0.26.0 `deleteAllPendingCmd`.
pub(in crate::broker::redis::scripts) const DELETE_ALL_PENDING_TASKS_SOURCE: &str = r#"
local ids = redis.call("LRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
  redis.call("DEL", ARGV[1] .. id)
end
redis.call("DEL", KEYS[1])
return table.getn(ids)
"#;
