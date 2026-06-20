// Source: Asynq v0.26.0 `forwardCmd`.
pub(in crate::broker::redis::scripts) const FORWARD_SOURCE: &str = r#"
local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, 100)
for _, id in ipairs(ids) do
  local taskKey = ARGV[2] .. id
  local group = redis.call("HGET", taskKey, "group")
  if group and group ~= '' then
    redis.call("ZADD", ARGV[4] .. group, ARGV[1], id)
    redis.call("ZREM", KEYS[1], id)
    redis.call("HSET", taskKey,
               "state", "aggregating")
  else
    redis.call("LPUSH", KEYS[2], id)
    redis.call("ZREM", KEYS[1], id)
    redis.call("HSET", taskKey,
               "state", "pending",
               "pending_since", ARGV[3])
  end
end
return table.getn(ids)
"#;

// Source: Asynq v0.26.0 `deleteExpiredCompletedTasksCmd`.
pub(in crate::broker::redis::scripts) const DELETE_EXPIRED_COMPLETED_TASKS_SOURCE: &str = r#"
local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, tonumber(ARGV[3]))
for _, id in ipairs(ids) do
	redis.call("DEL", ARGV[2] .. id)
	redis.call("ZREM", KEYS[1], id)
end
return table.getn(ids)
"#;

// Source: Asynq v0.26.0 `listLeaseExpiredCmd`.
pub(in crate::broker::redis::scripts) const LIST_LEASE_EXPIRED_SOURCE: &str = r#"
local res = {}
local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
for _, id in ipairs(ids) do
  local key = ARGV[2] .. id
  local v = redis.call("HGET", key, "msg")
  if v then
    table.insert(res, v)
  end
end
return res
"#;
