// Source: Asynq v0.26.0 `removeQueueCmd`:
// https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1775-L1852
pub(in crate::broker::redis::scripts) const DELETE_QUEUE_SOURCE: &str = r#"
local ids = {}
for _, id in ipairs(redis.call("LRANGE", KEYS[1], 0, -1)) do
	table.insert(ids, id)
end
for _, id in ipairs(redis.call("LRANGE", KEYS[2], 0, -1)) do
	table.insert(ids, id)
end
for _, id in ipairs(redis.call("ZRANGE", KEYS[3], 0, -1)) do
	table.insert(ids, id)
end
for _, id in ipairs(redis.call("ZRANGE", KEYS[4], 0, -1)) do
	table.insert(ids, id)
end
for _, id in ipairs(redis.call("ZRANGE", KEYS[5], 0, -1)) do
	table.insert(ids, id)
end
if table.getn(ids) > 0 then
	return -1
end
for _, id in ipairs(ids) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(ids) do
	redis.call("DEL", ARGV[1] .. id)
end
redis.call("DEL", KEYS[1])
redis.call("DEL", KEYS[2])
redis.call("DEL", KEYS[3])
redis.call("DEL", KEYS[4])
redis.call("DEL", KEYS[5])
redis.call("DEL", KEYS[6])
return 1
"#;

// Source: Asynq v0.26.0 `removeQueueForceCmd`:
// https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1775-L1826
pub(in crate::broker::redis::scripts) const DELETE_QUEUE_FORCE_SOURCE: &str = r#"
local active = redis.call("LLEN", KEYS[2])
if active > 0 then
	return -2
end
for _, id in ipairs(redis.call("LRANGE", KEYS[1], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("LRANGE", KEYS[2], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("ZRANGE", KEYS[3], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("ZRANGE", KEYS[4], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("ZRANGE", KEYS[5], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("LRANGE", KEYS[1], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("LRANGE", KEYS[2], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("ZRANGE", KEYS[3], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("ZRANGE", KEYS[4], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
for _, id in ipairs(redis.call("ZRANGE", KEYS[5], 0, -1)) do
	redis.call("DEL", ARGV[1] .. id)
end
redis.call("DEL", KEYS[1])
redis.call("DEL", KEYS[2])
redis.call("DEL", KEYS[3])
redis.call("DEL", KEYS[4])
redis.call("DEL", KEYS[5])
redis.call("DEL", KEYS[6])
return 1
"#;
