// Source: Asynq v0.26.0 inspector list task commands:
// https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L670-L721
pub(in crate::broker::redis::scripts) const LIST_TASKS_SOURCE: &str = r#"
local ids = {}
if ARGV[4] == "1" then
  ids = redis.call("LRANGE", KEYS[1], ARGV[1], ARGV[2])
else
  ids = redis.call("ZRANGE", KEYS[1], ARGV[1], ARGV[2], "WITHSCORES")
end
local res = {}
if ARGV[4] == "1" then
  for _, id in ipairs(ids) do
    local msg, result = unpack(redis.call("HMGET", ARGV[3] .. id, "msg", "result"))
    if msg then
      table.insert(res, msg)
      table.insert(res, "")
      table.insert(res, result or "")
    end
  end
else
  for i = 1, table.getn(ids), 2 do
    local id = ids[i]
    local score = ids[i + 1]
    local msg, result = unpack(redis.call("HMGET", ARGV[3] .. id, "msg", "result"))
    if msg then
      table.insert(res, msg)
      table.insert(res, score)
      table.insert(res, result or "")
    end
  end
end
return res
"#;
