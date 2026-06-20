// Source: Asynq v0.26.0 `currentStatsCmd`.
pub(in crate::broker::redis::scripts) const CURRENT_QUEUE_STATS_SOURCE: &str = r#"
local res = {}
local pendingTaskCount = redis.call("LLEN", KEYS[1])
table.insert(res, KEYS[1])
table.insert(res, pendingTaskCount)
table.insert(res, KEYS[2])
table.insert(res, redis.call("LLEN", KEYS[2]))
table.insert(res, KEYS[3])
table.insert(res, redis.call("ZCARD", KEYS[3]))
table.insert(res, KEYS[4])
table.insert(res, redis.call("ZCARD", KEYS[4]))
table.insert(res, KEYS[5])
table.insert(res, redis.call("ZCARD", KEYS[5]))
table.insert(res, KEYS[6])
table.insert(res, redis.call("ZCARD", KEYS[6]))
for i=7,10 do
  local count = 0
  local n = redis.call("GET", KEYS[i])
  if n then
    count = tonumber(n)
  end
  table.insert(res, KEYS[i])
  table.insert(res, count)
end
table.insert(res, KEYS[11])
table.insert(res, redis.call("EXISTS", KEYS[11]))
table.insert(res, "oldest_pending_since")
if pendingTaskCount > 0 then
  local id = redis.call("LRANGE", KEYS[1], -1, -1)[1]
  table.insert(res, redis.call("HGET", ARGV[1] .. id, "pending_since"))
else
  table.insert(res, 0)
end
local group_names = redis.call("SMEMBERS", KEYS[12])
table.insert(res, "group_size")
table.insert(res, table.getn(group_names))
local aggregating_count = 0
for _, gname in ipairs(group_names) do
  aggregating_count = aggregating_count + redis.call("ZCARD", ARGV[2] .. gname)
end
table.insert(res, "aggregating_count")
table.insert(res, aggregating_count)
return res
"#;

// Source: Asynq v0.26.0 `memoryUsageCmd`.
pub(in crate::broker::redis::scripts) const QUEUE_MEMORY_USAGE_SOURCE: &str = r#"
local sample_size = tonumber(ARGV[2])
if sample_size <= 0 then
  return redis.error_reply("sample size must be a positive number")
end
local memusg = 0
for i=1,2 do
  local ids = redis.call("LRANGE", KEYS[i], 0, sample_size - 1)
  local sample_total = 0
  if (table.getn(ids) > 0) then
    for _, id in ipairs(ids) do
      local bytes = redis.call("MEMORY", "USAGE", ARGV[1] .. id)
      sample_total = sample_total + bytes
    end
    local n = redis.call("LLEN", KEYS[i])
    local avg = sample_total / table.getn(ids)
    memusg = memusg + (avg * n)
  end
  local m = redis.call("MEMORY", "USAGE", KEYS[i])
  if (m) then
    memusg = memusg + m
  end
end
for i=3,6 do
  local ids = redis.call("ZRANGE", KEYS[i], 0, sample_size - 1)
  local sample_total = 0
  if (table.getn(ids) > 0) then
    for _, id in ipairs(ids) do
      local bytes = redis.call("MEMORY", "USAGE", ARGV[1] .. id)
      sample_total = sample_total + bytes
    end
    local n = redis.call("ZCARD", KEYS[i])
    local avg = sample_total / table.getn(ids)
    memusg = memusg + (avg * n)
  end
  local m = redis.call("MEMORY", "USAGE", KEYS[i])
  if (m) then
    memusg = memusg + m
  end
end
local groups = redis.call("SMEMBERS", KEYS[7])
if table.getn(groups) > 0 then
  local agg_task_count = 0
  local agg_task_sample_total = 0
  local agg_task_sample_size = 0
  for i, gname in ipairs(groups) do
    local group_key = ARGV[4] .. gname
    agg_task_count = agg_task_count + redis.call("ZCARD", group_key)
    if i <= tonumber(ARGV[3]) then
      local ids = redis.call("ZRANGE", group_key, 0, sample_size - 1)
      for _, id in ipairs(ids) do
        local bytes = redis.call("MEMORY", "USAGE", ARGV[1] .. id)
        agg_task_sample_total = agg_task_sample_total + bytes
        agg_task_sample_size = agg_task_sample_size + 1
      end
    end
  end
  local avg = agg_task_sample_total / agg_task_sample_size
  memusg = memusg + (avg * agg_task_count)
end
return memusg
"#;

// Source: Asynq v0.26.0 `historicalStatsCmd`.
pub(in crate::broker::redis::scripts) const HISTORICAL_QUEUE_STATS_SOURCE: &str = r#"
local res = {}
for _, key in ipairs(KEYS) do
  local n = redis.call("GET", key)
  if not n then
    n = 0
  end
  table.insert(res, tonumber(n))
end
return res
"#;

// Source: Asynq v0.26.0 `groupStatsCmd`.
pub(in crate::broker::redis::scripts) const GROUP_STATS_SOURCE: &str = r#"
local res = {}
local group_names = redis.call("SMEMBERS", KEYS[1])
for _, gname in ipairs(group_names) do
  local size = redis.call("ZCARD", ARGV[1] .. gname)
  table.insert(res, gname)
  table.insert(res, size)
end
return res
"#;
