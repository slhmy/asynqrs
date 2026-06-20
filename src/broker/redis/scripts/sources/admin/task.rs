// Source: Asynq v0.26.0 `deleteTaskCmd`.
pub(in crate::broker::redis::scripts) const DELETE_TASK_SOURCE: &str = r#"
if redis.call("EXISTS", KEYS[1]) == 0 then
  return 0
end
local state, group = unpack(redis.call("HMGET", KEYS[1], "state", "group"))
if state == "active" then
  return -1
end
if state == "pending" then
  if redis.call("LREM", ARGV[2] .. state, 0, ARGV[1]) == 0 then
    return redis.error_reply("task is not found in list: " .. tostring(ARGV[2] .. state))
  end
elseif state == "aggregating" then
  if redis.call("ZREM", ARGV[3] .. group, ARGV[1]) == 0 then
    return redis.error_reply("task is not found in zset: " .. tostring(ARGV[3] .. group))
  end
  if redis.call("ZCARD", ARGV[3] .. group) == 0 then
    redis.call("SREM", KEYS[2], group)
  end
else
  if redis.call("ZREM", ARGV[2] .. state, ARGV[1]) == 0 then
    return redis.error_reply("task is not found in zset: " .. tostring(ARGV[2] .. state))
  end
end
local unique_key = redis.call("HGET", KEYS[1], "unique_key")
if unique_key and unique_key ~= "" and redis.call("GET", unique_key) == ARGV[1] then
  redis.call("DEL", unique_key)
end
return redis.call("DEL", KEYS[1])
"#;

// Source: Asynq v0.26.0 `runTaskCmd`.
pub(in crate::broker::redis::scripts) const RUN_TASK_SOURCE: &str = r#"
if redis.call("EXISTS", KEYS[1]) == 0 then
  return 0
end
local state, group = unpack(redis.call("HMGET", KEYS[1], "state", "group"))
if state == "active" then
  return -1
elseif state == "pending" then
  return -2
elseif state == "aggregating" then
  local n = redis.call("ZREM", ARGV[3] .. group, ARGV[1])
  if n == 0 then
    return redis.error_reply("internal error: task id not found in zset " .. tostring(ARGV[3] .. group))
  end
  if redis.call("ZCARD", ARGV[3] .. group) == 0 then
    redis.call("SREM", KEYS[3], group)
  end
else
  local n = redis.call("ZREM", ARGV[2] .. state, ARGV[1])
  if n == 0 then
    return redis.error_reply("internal error: task id not found in zset " .. tostring(ARGV[2] .. state))
  end
end
redis.call("LPUSH", KEYS[2], ARGV[1])
redis.call("HSET", KEYS[1], "state", "pending")
return 1
"#;

// Source: Asynq v0.26.0 `archiveTaskCmd`.
pub(in crate::broker::redis::scripts) const ARCHIVE_TASK_SOURCE: &str = r#"
if redis.call("EXISTS", KEYS[1]) == 0 then
  return 0
end
local state, group = unpack(redis.call("HMGET", KEYS[1], "state", "group"))
if state == "active" then
  return -2
end
if state == "archived" then
  return -1
end
if state == "pending" then
  if redis.call("LREM", ARGV[5] .. state, 1, ARGV[1]) == 0 then
    return redis.error_reply("task id not found in list " .. tostring(ARGV[5] .. state))
  end
elseif state == "aggregating" then
  if redis.call("ZREM", ARGV[6] .. group, ARGV[1]) == 0 then
    return redis.error_reply("task id not found in zset " .. tostring(ARGV[6] .. group))
  end
  if redis.call("ZCARD", ARGV[6] .. group) == 0 then
    redis.call("SREM", KEYS[3], group)
  end
else
  if redis.call("ZREM", ARGV[5] .. state, ARGV[1]) == 0 then
    return redis.error_reply("task id not found in zset " .. tostring(ARGV[5] .. state))
  end
end
redis.call("ZADD", KEYS[2], ARGV[2], ARGV[1])
redis.call("HSET", KEYS[1], "state", "archived")
redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", ARGV[3])
redis.call("ZREMRANGEBYRANK", KEYS[2], 0, -ARGV[4])
return 1
"#;

// Source: Asynq v0.26.0 `updateTaskPayloadCmd`:
// https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1415-L1450
pub(in crate::broker::redis::scripts) const UPDATE_TASK_PAYLOAD_SOURCE: &str = r#"
if redis.call("EXISTS", KEYS[1]) == 0 then
  return 0
end
local state, pending_since, group, unique_key = unpack(redis.call("HMGET", KEYS[1], "state", "pending_since", "group", "unique_key"))
if state ~= "scheduled" then
  return -1
end
local redis_call_args = {"state", state}
if pending_since then
  table.insert(redis_call_args, "pending_since")
  table.insert(redis_call_args, pending_since)
end
if group then
  table.insert(redis_call_args, "group")
  table.insert(redis_call_args, group)
end
if unique_key then
  table.insert(redis_call_args, "unique_key")
  table.insert(redis_call_args, unique_key)
end
redis.call("HSET", KEYS[1], "msg", ARGV[1], unpack(redis_call_args))
return 1
"#;

// Source: Asynq v0.26.0 `getTaskInfoCmd`.
pub(in crate::broker::redis::scripts) const TASK_INFO_SOURCE: &str = r#"
if redis.call("EXISTS", KEYS[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
local msg, state, result = unpack(redis.call("HMGET", KEYS[1], "msg", "state", "result"))
if state == "scheduled" or state == "retry" then
  return {msg, state, redis.call("ZSCORE", ARGV[3] .. state, ARGV[1]), result}
end
if state == "pending" then
  return {msg, state, ARGV[2], result}
end
return {msg, state, 0, result}
"#;
