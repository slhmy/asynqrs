// Source: Asynq v0.26.0 `addToGroupCmd`.
pub(in crate::broker::redis::scripts) const ADD_TO_GROUP_SOURCE: &str = r#"
if redis.call("EXISTS", KEYS[1]) == 1 then
	return 0
end
redis.call("HSET", KEYS[1],
           "msg", ARGV[1],
           "state", "aggregating",
	       "group", ARGV[4])
redis.call("ZADD", KEYS[2], ARGV[3], ARGV[2])
redis.call("SADD", KEYS[3], ARGV[4])
return 1
"#;
// Source: Asynq v0.26.0 `addToGroupUniqueCmd`.
pub(in crate::broker::redis::scripts) const ADD_TO_GROUP_UNIQUE_SOURCE: &str = r#"
local ok = redis.call("SET", KEYS[4], ARGV[2], "NX", "EX", ARGV[5])
if not ok then
  return -1
end
if redis.call("EXISTS", KEYS[1]) == 1 then
	return 0
end
redis.call("HSET", KEYS[1],
           "msg", ARGV[1],
           "state", "aggregating",
	       "group", ARGV[4])
redis.call("ZADD", KEYS[2], ARGV[3], ARGV[2])
redis.call("SADD", KEYS[3], ARGV[4])
return 1
"#;
// Source: Asynq v0.26.0 `aggregationCheckCmd`.
pub(in crate::broker::redis::scripts) const AGGREGATION_CHECK_SOURCE: &str = r#"
local size = redis.call("ZCARD", KEYS[1])
if size == 0 then
	return 0
end
local maxSize = tonumber(ARGV[1])
if maxSize ~= 0 and size >= maxSize then
	local res = redis.call("ZRANGE", KEYS[1], 0, maxSize-1, "WITHSCORES")
	for i=1, table.getn(res)-1, 2 do
		redis.call("ZADD", KEYS[2], tonumber(res[i+1]), res[i])
	end
	redis.call("ZREMRANGEBYRANK", KEYS[1], 0, maxSize-1)
	redis.call("ZADD", KEYS[3], ARGV[4], KEYS[2])
	if size == maxSize then
		redis.call("SREM", KEYS[4], ARGV[6])
	end
	return 1
end
local maxDelay = tonumber(ARGV[2])
local currentTime = tonumber(ARGV[5])
if maxDelay ~= 0 then
	local oldestEntry = redis.call("ZRANGE", KEYS[1], 0, 0, "WITHSCORES")
	local oldestEntryScore = tonumber(oldestEntry[2])
	local maxDelayTime = currentTime - maxDelay
	if oldestEntryScore <= maxDelayTime then
		local res = redis.call("ZRANGE", KEYS[1], 0, maxSize-1, "WITHSCORES")
		for i=1, table.getn(res)-1, 2 do
			redis.call("ZADD", KEYS[2], tonumber(res[i+1]), res[i])
		end
		redis.call("ZREMRANGEBYRANK", KEYS[1], 0, maxSize-1)
		redis.call("ZADD", KEYS[3], ARGV[4], KEYS[2])
		if size <= maxSize or maxSize == 0 then
			redis.call("SREM", KEYS[4], ARGV[6])
		end
		return 1
	end
end
local latestEntry = redis.call("ZREVRANGE", KEYS[1], 0, 0, "WITHSCORES")
local latestEntryScore = tonumber(latestEntry[2])
local gracePeriodStartTime = currentTime - tonumber(ARGV[3])
if latestEntryScore <= gracePeriodStartTime then
	local res = redis.call("ZRANGE", KEYS[1], 0, maxSize-1, "WITHSCORES")
	for i=1, table.getn(res)-1, 2 do
		redis.call("ZADD", KEYS[2], tonumber(res[i+1]), res[i])
	end
	redis.call("ZREMRANGEBYRANK", KEYS[1], 0, maxSize-1)
	redis.call("ZADD", KEYS[3], ARGV[4], KEYS[2])
	if size <= maxSize or maxSize == 0 then
		redis.call("SREM", KEYS[4], ARGV[6])
	end
	return 1
end
return 0
"#;
// Source: Asynq v0.26.0 `readAggregationSetCmd`.
pub(in crate::broker::redis::scripts) const READ_AGGREGATION_SET_SOURCE: &str = r#"
local msgs = {}
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
	local key = ARGV[1] .. id
	table.insert(msgs, redis.call("HGET", key, "msg"))
end
return msgs
"#;
// Source: Asynq v0.26.0 `deleteAggregationSetCmd`.
pub(in crate::broker::redis::scripts) const DELETE_AGGREGATION_SET_SOURCE: &str = r#"
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids)  do
	redis.call("DEL", ARGV[1] .. id)
end
redis.call("DEL", KEYS[1])
redis.call("ZREM", KEYS[2], KEYS[1])
return redis.status_reply("OK")
"#;
// Source: Asynq v0.26.0 `reclaimStateAggregationSetsCmd`.
pub(in crate::broker::redis::scripts) const RECLAIM_STALE_AGGREGATION_SETS_SOURCE: &str = r#"
local staleSetKeys = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
for _, key in ipairs(staleSetKeys) do
	local idx = string.find(key, ":[^:]*$")
	local groupKey = string.sub(key, 1, idx-1)
	local res = redis.call("ZRANGE", key, 0, -1, "WITHSCORES")
	for i=1, table.getn(res)-1, 2 do
		redis.call("ZADD", groupKey, tonumber(res[i+1]), res[i])
	end
	redis.call("DEL", key)
end
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
return redis.status_reply("OK")
"#;
// Source: Asynq v0.26.0 `runAllAggregatingCmd`.
pub(in crate::broker::redis::scripts) const RUN_ALL_AGGREGATING_TASKS_SOURCE: &str = r#"
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
  redis.call("LPUSH", KEYS[2], id)
  redis.call("HSET", ARGV[1] .. id, "state", "pending")
end
redis.call("DEL", KEYS[1])
redis.call("SREM", KEYS[3], ARGV[2])
return table.getn(ids)
"#;
// Source: Asynq v0.26.0 `archiveAllAggregatingCmd`.
pub(in crate::broker::redis::scripts) const ARCHIVE_ALL_AGGREGATING_TASKS_SOURCE: &str = r#"
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
  redis.call("ZADD", KEYS[2], ARGV[1], id)
  redis.call("HSET", ARGV[4] .. id, "state", "archived")
end
redis.call("ZREMRANGEBYSCORE", KEYS[2], "-inf", ARGV[2])
redis.call("ZREMRANGEBYRANK", KEYS[2], 0, -ARGV[3])
redis.call("DEL", KEYS[1])
redis.call("SREM", KEYS[3], ARGV[5])
return table.getn(ids)
"#;
// Source: Asynq v0.26.0 `deleteAllAggregatingCmd`.
pub(in crate::broker::redis::scripts) const DELETE_ALL_AGGREGATING_TASKS_SOURCE: &str = r#"
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
  redis.call("DEL", ARGV[1] .. id)
end
redis.call("SREM", KEYS[2], ARGV[2])
redis.call("DEL", KEYS[1])
return table.getn(ids)
"#;
