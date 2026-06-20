// Source: Asynq v0.26.0 `retryCmd`.
pub(in crate::broker::redis::scripts) const RETRY_SOURCE: &str = r#"
if redis.call("LREM", KEYS[2], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[3], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
redis.call("ZADD", KEYS[4], ARGV[3], ARGV[1])
redis.call("HSET", KEYS[1], "msg", ARGV[2], "state", "retry")
if tonumber(ARGV[5]) == 1 then
	local n = redis.call("INCR", KEYS[5])
	if tonumber(n) == 1 then
		redis.call("EXPIREAT", KEYS[5], ARGV[4])
	end
	local m = redis.call("INCR", KEYS[6])
	if tonumber(m) == 1 then
		redis.call("EXPIREAT", KEYS[6], ARGV[4])
	end
	local total = redis.call("GET", KEYS[7])
	if tonumber(total) == tonumber(ARGV[6]) then
		redis.call("SET", KEYS[7], 1)
		redis.call("SET", KEYS[8], 1)
	else
		redis.call("INCR", KEYS[7])
		redis.call("INCR", KEYS[8])
	end
end
return redis.status_reply("OK")
"#;

// Source: Asynq v0.26.0 `archiveCmd`.
// `ZRANGEBYSCORE` is equivalent to upstream's Redis 6.2 `ZRANGE ... BYSCORE`
// form while retaining the repository's Redis 5 integration-test support.
pub(in crate::broker::redis::scripts) const ARCHIVE_SOURCE: &str = r#"
if redis.call("LREM", KEYS[2], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[3], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
redis.call("ZADD", KEYS[4], ARGV[3], ARGV[1])
local old = redis.call("ZRANGEBYSCORE", KEYS[4], "-inf", ARGV[4])
if #old > 0 then
	for _, id in ipairs(old) do
		redis.call("DEL", KEYS[9] .. id)
	end
	redis.call("ZREM", KEYS[4], unpack(old))
end

local extra = redis.call("ZRANGE", KEYS[4], 0, -ARGV[5])
if #extra > 0 then
	for _, id in ipairs(extra) do
		redis.call("DEL", KEYS[9] .. id)
	end
	redis.call("ZREM", KEYS[4], unpack(extra))
end

redis.call("HSET", KEYS[1], "msg", ARGV[2], "state", "archived")
local n = redis.call("INCR", KEYS[5])
if tonumber(n) == 1 then
	redis.call("EXPIREAT", KEYS[5], ARGV[6])
end
local m = redis.call("INCR", KEYS[6])
if tonumber(m) == 1 then
	redis.call("EXPIREAT", KEYS[6], ARGV[6])
end
local total = redis.call("GET", KEYS[7])
if tonumber(total) == tonumber(ARGV[7]) then
	redis.call("SET", KEYS[7], 1)
	redis.call("SET", KEYS[8], 1)
else
	redis.call("INCR", KEYS[7])
	redis.call("INCR", KEYS[8])
end
return redis.status_reply("OK")
"#;

// Source: Asynq v0.26.0 `requeueCmd`.
pub(in crate::broker::redis::scripts) const REQUEUE_SOURCE: &str = r#"
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
redis.call("RPUSH", KEYS[3], ARGV[1])
redis.call("HSET", KEYS[4], "state", "pending")
return redis.status_reply("OK")
"#;
