use crate::{RedisArg, RedisScript, RedisScriptCall};
use thiserror::Error;

/// Metadata and source for Asynq task lifecycle Lua scripts.
///
/// Reference: Asynq v0.26.0 task lifecycle Lua scripts:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L735>.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RedisScriptSpec {
    script: RedisScript,
    name: &'static str,
    source: &'static str,
    key_count: usize,
    arg_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisScriptResult {
    Success,
    TaskIdConflict,
    DuplicateTask,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RedisScriptCallError {
    #[error("{} script expected {expected} keys, got {actual}", script.name())]
    WrongKeyCount {
        script: RedisScript,
        expected: usize,
        actual: usize,
    },
    #[error("{} script expected {expected} args, got {actual}", script.name())]
    WrongArgCount {
        script: RedisScript,
        expected: usize,
        actual: usize,
    },
}

impl RedisScript {
    pub const ALL: [Self; 16] = [
        Self::Enqueue,
        Self::EnqueueUnique,
        Self::Schedule,
        Self::ScheduleUnique,
        Self::AddToGroup,
        Self::AddToGroupUnique,
        Self::Dequeue,
        Self::Done,
        Self::DoneUnique,
        Self::MarkAsComplete,
        Self::MarkAsCompleteUnique,
        Self::Retry,
        Self::Archive,
        Self::Requeue,
        Self::Forward,
        Self::ListLeaseExpired,
    ];

    pub const fn spec(self) -> RedisScriptSpec {
        match self {
            Self::Enqueue => RedisScriptSpec {
                script: self,
                name: "enqueue",
                source: ENQUEUE_SOURCE,
                key_count: 2,
                arg_count: 3,
            },
            Self::EnqueueUnique => RedisScriptSpec {
                script: self,
                name: "enqueue_unique",
                source: ENQUEUE_UNIQUE_SOURCE,
                key_count: 3,
                arg_count: 4,
            },
            Self::Schedule => RedisScriptSpec {
                script: self,
                name: "schedule",
                source: SCHEDULE_SOURCE,
                key_count: 2,
                arg_count: 3,
            },
            Self::ScheduleUnique => RedisScriptSpec {
                script: self,
                name: "schedule_unique",
                source: SCHEDULE_UNIQUE_SOURCE,
                key_count: 3,
                arg_count: 4,
            },
            Self::AddToGroup => RedisScriptSpec {
                script: self,
                name: "add_to_group",
                source: ADD_TO_GROUP_SOURCE,
                key_count: 3,
                arg_count: 4,
            },
            Self::AddToGroupUnique => RedisScriptSpec {
                script: self,
                name: "add_to_group_unique",
                source: ADD_TO_GROUP_UNIQUE_SOURCE,
                key_count: 4,
                arg_count: 5,
            },
            Self::Dequeue => RedisScriptSpec {
                script: self,
                name: "dequeue",
                source: DEQUEUE_SOURCE,
                key_count: 5,
                arg_count: 1,
            },
            Self::Done => RedisScriptSpec {
                script: self,
                name: "done",
                source: DONE_SOURCE,
                key_count: 5,
                arg_count: 3,
            },
            Self::DoneUnique => RedisScriptSpec {
                script: self,
                name: "done_unique",
                source: DONE_UNIQUE_SOURCE,
                key_count: 6,
                arg_count: 3,
            },
            Self::MarkAsComplete => RedisScriptSpec {
                script: self,
                name: "mark_as_complete",
                source: MARK_AS_COMPLETE_SOURCE,
                key_count: 6,
                arg_count: 5,
            },
            Self::MarkAsCompleteUnique => RedisScriptSpec {
                script: self,
                name: "mark_as_complete_unique",
                source: MARK_AS_COMPLETE_UNIQUE_SOURCE,
                key_count: 7,
                arg_count: 5,
            },
            Self::Retry => RedisScriptSpec {
                script: self,
                name: "retry",
                source: RETRY_SOURCE,
                key_count: 8,
                arg_count: 6,
            },
            Self::Archive => RedisScriptSpec {
                script: self,
                name: "archive",
                source: ARCHIVE_SOURCE,
                key_count: 8,
                arg_count: 6,
            },
            Self::Requeue => RedisScriptSpec {
                script: self,
                name: "requeue",
                source: REQUEUE_SOURCE,
                key_count: 4,
                arg_count: 1,
            },
            Self::Forward => RedisScriptSpec {
                script: self,
                name: "forward",
                source: FORWARD_SOURCE,
                key_count: 3,
                arg_count: 2,
            },
            Self::ListLeaseExpired => RedisScriptSpec {
                script: self,
                name: "list_lease_expired",
                source: LIST_LEASE_EXPIRED_SOURCE,
                key_count: 2,
                arg_count: 1,
            },
        }
    }

    pub const fn name(self) -> &'static str {
        self.spec().name
    }

    pub const fn source(self) -> &'static str {
        self.spec().source
    }

    pub const fn key_count(self) -> usize {
        self.spec().key_count
    }

    pub const fn arg_count(self) -> usize {
        self.spec().arg_count
    }

    pub const fn result_for_code(self, code: i64) -> Option<RedisScriptResult> {
        if !self.supports_integer_result() {
            return None;
        }
        match code {
            1 => Some(RedisScriptResult::Success),
            0 => Some(RedisScriptResult::TaskIdConflict),
            -1 if self.supports_duplicate_result() => Some(RedisScriptResult::DuplicateTask),
            _ => None,
        }
    }

    pub const fn supports_integer_result(self) -> bool {
        matches!(
            self,
            Self::Enqueue
                | Self::EnqueueUnique
                | Self::Schedule
                | Self::ScheduleUnique
                | Self::AddToGroup
                | Self::AddToGroupUnique
        )
    }

    pub const fn supports_duplicate_result(self) -> bool {
        matches!(
            self,
            Self::EnqueueUnique | Self::ScheduleUnique | Self::AddToGroupUnique
        )
    }

    pub fn validate_call(
        self,
        keys: &[String],
        args: &[RedisArg],
    ) -> Result<(), RedisScriptCallError> {
        let spec = self.spec();
        if keys.len() != spec.key_count {
            return Err(RedisScriptCallError::WrongKeyCount {
                script: self,
                expected: spec.key_count,
                actual: keys.len(),
            });
        }
        if args.len() != spec.arg_count {
            return Err(RedisScriptCallError::WrongArgCount {
                script: self,
                expected: spec.arg_count,
                actual: args.len(),
            });
        }
        Ok(())
    }
}

impl RedisScriptCall {
    pub fn validate(&self) -> Result<(), RedisScriptCallError> {
        self.script().validate_call(self.keys(), self.args())
    }
}

impl RedisScriptSpec {
    pub const fn script(self) -> RedisScript {
        self.script
    }

    pub const fn name(self) -> &'static str {
        self.name
    }

    pub const fn source(self) -> &'static str {
        self.source
    }

    pub const fn key_count(self) -> usize {
        self.key_count
    }

    pub const fn arg_count(self) -> usize {
        self.arg_count
    }
}

// Source: Asynq v0.26.0 `enqueueCmd`.
const ENQUEUE_SOURCE: &str = r#"
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
const ENQUEUE_UNIQUE_SOURCE: &str = r#"
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
const SCHEDULE_SOURCE: &str = r#"
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
const SCHEDULE_UNIQUE_SOURCE: &str = r#"
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

// Source: Asynq v0.26.0 `addToGroupCmd`.
const ADD_TO_GROUP_SOURCE: &str = r#"
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
const ADD_TO_GROUP_UNIQUE_SOURCE: &str = r#"
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

// Source: Asynq v0.26.0 `dequeueCmd`.
const DEQUEUE_SOURCE: &str = r#"
if redis.call("EXISTS", KEYS[5]) == 1 then
  return nil
end
local id = redis.call("RPOPLPUSH", KEYS[1], KEYS[2])
if id then
  local key = KEYS[4] .. id
  redis.call("HSET", key, "state", "active")
  redis.call("HDEL", key, "pending_since")
  redis.call("ZADD", KEYS[3], ARGV[1], id)
  return redis.call("HGET", key, "msg")
end
return nil
"#;

// Source: Asynq v0.26.0 `doneCmd`.
const DONE_SOURCE: &str = r#"
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("DEL", KEYS[3]) == 0 then
  return redis.error_reply("NOT FOUND")
end
local n = redis.call("INCR", KEYS[4])
if tonumber(n) == 1 then
	redis.call("EXPIREAT", KEYS[4], ARGV[2])
end
local total = redis.call("GET", KEYS[5])
if tonumber(total) == tonumber(ARGV[3]) then
	redis.call("SET", KEYS[5], 1)
else
	redis.call("INCR", KEYS[5])
end
return redis.status_reply("OK")
"#;

// Source: Asynq v0.26.0 `doneUniqueCmd`.
const DONE_UNIQUE_SOURCE: &str = r#"
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("DEL", KEYS[3]) == 0 then
  return redis.error_reply("NOT FOUND")
end
local n = redis.call("INCR", KEYS[4])
if tonumber(n) == 1 then
	redis.call("EXPIREAT", KEYS[4], ARGV[2])
end
local total = redis.call("GET", KEYS[5])
if tonumber(total) == tonumber(ARGV[3]) then
	redis.call("SET", KEYS[5], 1)
else
	redis.call("INCR", KEYS[5])
end
if redis.call("GET", KEYS[6]) == ARGV[1] then
  redis.call("DEL", KEYS[6])
end
return redis.status_reply("OK")
"#;

// Source: Asynq v0.26.0 `markAsCompleteCmd`.
const MARK_AS_COMPLETE_SOURCE: &str = r#"
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZADD", KEYS[3], ARGV[3], ARGV[1]) ~= 1 then
  return redis.error_reply("INTERNAL")
end
redis.call("HSET", KEYS[4], "msg", ARGV[4], "state", "completed")
local n = redis.call("INCR", KEYS[5])
if tonumber(n) == 1 then
	redis.call("EXPIREAT", KEYS[5], ARGV[2])
end
local total = redis.call("GET", KEYS[6])
if tonumber(total) == tonumber(ARGV[5]) then
	redis.call("SET", KEYS[6], 1)
else
	redis.call("INCR", KEYS[6])
end
return redis.status_reply("OK")
"#;

// Source: Asynq v0.26.0 `markAsCompleteUniqueCmd`.
const MARK_AS_COMPLETE_UNIQUE_SOURCE: &str = r#"
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZADD", KEYS[3], ARGV[3], ARGV[1]) ~= 1 then
  return redis.error_reply("INTERNAL")
end
redis.call("HSET", KEYS[4], "msg", ARGV[4], "state", "completed")
local n = redis.call("INCR", KEYS[5])
if tonumber(n) == 1 then
	redis.call("EXPIREAT", KEYS[5], ARGV[2])
end
local total = redis.call("GET", KEYS[6])
if tonumber(total) == tonumber(ARGV[5]) then
	redis.call("SET", KEYS[6], 1)
else
	redis.call("INCR", KEYS[6])
end
if redis.call("GET", KEYS[7]) == ARGV[1] then
  redis.call("DEL", KEYS[7])
end
return redis.status_reply("OK")
"#;

// Source: Asynq v0.26.0 `retryCmd`.
const RETRY_SOURCE: &str = r#"
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
redis.call("ZADD", KEYS[3], ARGV[3], ARGV[1])
redis.call("HSET", KEYS[4],
           "msg", ARGV[2],
           "state", "retry")
if tonumber(ARGV[5]) == 1 then
  local processed = redis.call("INCR", KEYS[5])
  if tonumber(processed) == 1 then
    redis.call("EXPIREAT", KEYS[5], ARGV[4])
  end
  local processed_total = redis.call("GET", KEYS[6])
  if tonumber(processed_total) == tonumber(ARGV[6]) then
    redis.call("SET", KEYS[6], 1)
  else
    redis.call("INCR", KEYS[6])
  end
  local failed = redis.call("INCR", KEYS[7])
  if tonumber(failed) == 1 then
    redis.call("EXPIREAT", KEYS[7], ARGV[4])
  end
  local failed_total = redis.call("GET", KEYS[8])
  if tonumber(failed_total) == tonumber(ARGV[6]) then
    redis.call("SET", KEYS[8], 1)
  else
    redis.call("INCR", KEYS[8])
  end
end
return redis.status_reply("OK")
"#;

// Source: Asynq v0.26.0 archive task lifecycle script.
const ARCHIVE_SOURCE: &str = r#"
if redis.call("LREM", KEYS[1], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[2], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
redis.call("ZADD", KEYS[3], ARGV[3], ARGV[1])
redis.call("HSET", KEYS[4],
           "msg", ARGV[2],
           "state", "archived")
if tonumber(ARGV[5]) == 1 then
  local processed = redis.call("INCR", KEYS[5])
  if tonumber(processed) == 1 then
    redis.call("EXPIREAT", KEYS[5], ARGV[4])
  end
  local processed_total = redis.call("GET", KEYS[6])
  if tonumber(processed_total) == tonumber(ARGV[6]) then
    redis.call("SET", KEYS[6], 1)
  else
    redis.call("INCR", KEYS[6])
  end
  local failed = redis.call("INCR", KEYS[7])
  if tonumber(failed) == 1 then
    redis.call("EXPIREAT", KEYS[7], ARGV[4])
  end
  local failed_total = redis.call("GET", KEYS[8])
  if tonumber(failed_total) == tonumber(ARGV[6]) then
    redis.call("SET", KEYS[8], 1)
  else
    redis.call("INCR", KEYS[8])
  end
end
return redis.status_reply("OK")
"#;

// Source: Asynq v0.26.0 `requeueCmd`.
const REQUEUE_SOURCE: &str = r#"
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

// Source: Asynq v0.26.0 `forwardCmd`.
const FORWARD_SOURCE: &str = r#"
local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, 100)
for _, id in ipairs(ids) do
  local taskKey = KEYS[3] .. id
  local group = redis.call("HGET", taskKey, "group")
  if group then
    redis.call("HSET", taskKey, "state", "aggregating")
    redis.call("ZADD", KEYS[2] .. ":g:" .. group, ARGV[1], id)
    redis.call("SADD", KEYS[2] .. ":groups", group)
  else
    redis.call("HSET", taskKey, "state", "pending", "pending_since", ARGV[2])
    redis.call("LPUSH", KEYS[2], id)
  end
end
if next(ids) ~= nil then
  redis.call("ZREM", KEYS[1], unpack(ids))
end
return table.getn(ids)
"#;

// Source: Asynq v0.26.0 `listLeaseExpiredCmd`.
const LIST_LEASE_EXPIRED_SOURCE: &str = r#"
local res = {}
local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
for _, id in ipairs(ids) do
  local msg = redis.call("HGET", KEYS[2] .. id, "msg")
  if msg then
    table.insert(res, msg)
  end
end
return res
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    use crate::{
        EnqueuePlan, RedisArchivePlan, RedisCompletePlan, RedisEnqueueOperation, RedisEnqueuePlan,
        RedisExtendLeasePlan, RedisForwardPlan, RedisRecoverPlan, RedisRequeuePlan, RedisRetryPlan,
        Task, TaskMessage, TaskOption,
    };

    #[test]
    fn scripts_have_sources_and_shapes() {
        let expected = [
            (RedisScript::Enqueue, "enqueue", 2, 3),
            (RedisScript::EnqueueUnique, "enqueue_unique", 3, 4),
            (RedisScript::Schedule, "schedule", 2, 3),
            (RedisScript::ScheduleUnique, "schedule_unique", 3, 4),
            (RedisScript::AddToGroup, "add_to_group", 3, 4),
            (RedisScript::AddToGroupUnique, "add_to_group_unique", 4, 5),
            (RedisScript::Dequeue, "dequeue", 5, 1),
            (RedisScript::Done, "done", 5, 3),
            (RedisScript::DoneUnique, "done_unique", 6, 3),
            (RedisScript::MarkAsComplete, "mark_as_complete", 6, 5),
            (
                RedisScript::MarkAsCompleteUnique,
                "mark_as_complete_unique",
                7,
                5,
            ),
            (RedisScript::Retry, "retry", 8, 6),
            (RedisScript::Archive, "archive", 8, 6),
            (RedisScript::Requeue, "requeue", 4, 1),
            (RedisScript::Forward, "forward", 3, 2),
            (RedisScript::ListLeaseExpired, "list_lease_expired", 2, 1),
        ];

        for (script, name, key_count, arg_count) in expected {
            let spec = script.spec();
            assert_eq!(spec.script(), script);
            assert_eq!(spec.name(), name);
            assert_eq!(spec.key_count(), key_count);
            assert_eq!(spec.arg_count(), arg_count);
            assert!(spec.source().contains("redis.call"));
            match script {
                RedisScript::Dequeue => {
                    assert!(spec.source().contains("return nil"));
                    assert!(spec.source().contains("HGET"));
                }
                RedisScript::Forward => {
                    assert!(spec.source().contains("ZRANGEBYSCORE"));
                    assert!(spec.source().contains("table.getn"));
                }
                RedisScript::ListLeaseExpired => {
                    assert!(spec.source().contains("ZRANGEBYSCORE"));
                    assert!(spec.source().contains("HGET"));
                }
                RedisScript::Done
                | RedisScript::DoneUnique
                | RedisScript::MarkAsComplete
                | RedisScript::MarkAsCompleteUnique
                | RedisScript::Retry
                | RedisScript::Archive
                | RedisScript::Requeue => {
                    assert!(spec.source().contains("status_reply"));
                    assert!(spec.source().contains("NOT FOUND"));
                }
                _ => {
                    assert!(spec.source().contains("return 1"));
                }
            }
        }
    }

    #[test]
    fn scripts_map_return_codes() {
        assert_eq!(
            RedisScript::Enqueue.result_for_code(1),
            Some(RedisScriptResult::Success)
        );
        assert_eq!(
            RedisScript::Enqueue.result_for_code(0),
            Some(RedisScriptResult::TaskIdConflict)
        );
        assert_eq!(RedisScript::Enqueue.result_for_code(-1), None);
        assert_eq!(
            RedisScript::EnqueueUnique.result_for_code(-1),
            Some(RedisScriptResult::DuplicateTask)
        );
        assert_eq!(RedisScript::Done.result_for_code(1), None);
        assert_eq!(RedisScript::Retry.result_for_code(1), None);
        assert_eq!(RedisScript::Archive.result_for_code(1), None);
        assert_eq!(RedisScript::Requeue.result_for_code(1), None);
        assert_eq!(RedisScript::Forward.result_for_code(1), None);
        assert_eq!(RedisScript::ListLeaseExpired.result_for_code(1), None);
    }

    #[test]
    fn validates_script_call_shape() {
        let keys = vec!["k1".to_owned(), "k2".to_owned()];
        let args = vec![
            RedisArg::Bytes(Vec::new()),
            RedisArg::String("task-id".to_owned()),
            RedisArg::I64(1),
        ];

        assert_eq!(RedisScript::Enqueue.validate_call(&keys, &args), Ok(()));
        assert_eq!(
            RedisScript::Enqueue.validate_call(&keys[0..1], &args),
            Err(RedisScriptCallError::WrongKeyCount {
                script: RedisScript::Enqueue,
                expected: 2,
                actual: 1,
            })
        );
        assert_eq!(
            RedisScript::Enqueue.validate_call(&keys, &args[0..2]),
            Err(RedisScriptCallError::WrongArgCount {
                script: RedisScript::Enqueue,
                expected: 3,
                actual: 2,
            })
        );
    }

    #[test]
    fn redis_enqueue_plans_match_script_shapes() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let cases = [
            EnqueuePlan::from_task(
                &Task::new_with_options(
                    "email:welcome",
                    b"payload".to_vec(),
                    [TaskOption::queue("critical")],
                ),
                now,
                "pending-id",
            )
            .unwrap(),
            EnqueuePlan::from_task(
                &Task::new_with_options(
                    "email:welcome",
                    b"payload".to_vec(),
                    [
                        TaskOption::queue("critical"),
                        TaskOption::unique(Duration::from_secs(300)),
                    ],
                ),
                now,
                "pending-unique-id",
            )
            .unwrap(),
            EnqueuePlan::from_task(
                &Task::new_with_options(
                    "email:welcome",
                    b"payload".to_vec(),
                    [
                        TaskOption::queue("critical"),
                        TaskOption::process_in(Duration::from_secs(60)),
                    ],
                ),
                now,
                "scheduled-id",
            )
            .unwrap(),
            EnqueuePlan::from_task(
                &Task::new_with_options(
                    "email:welcome",
                    b"payload".to_vec(),
                    [
                        TaskOption::queue("critical"),
                        TaskOption::process_in(Duration::from_secs(60)),
                        TaskOption::unique(Duration::from_secs(300)),
                    ],
                ),
                now,
                "scheduled-unique-id",
            )
            .unwrap(),
            EnqueuePlan::from_task(
                &Task::new_with_options(
                    "email:welcome",
                    b"payload".to_vec(),
                    [TaskOption::queue("critical"), TaskOption::group("tenant-a")],
                ),
                now,
                "group-id",
            )
            .unwrap(),
            EnqueuePlan::from_task(
                &Task::new_with_options(
                    "email:welcome",
                    b"payload".to_vec(),
                    [
                        TaskOption::queue("critical"),
                        TaskOption::group("tenant-a"),
                        TaskOption::unique(Duration::from_secs(300)),
                    ],
                ),
                now,
                "group-unique-id",
            )
            .unwrap(),
        ];

        for plan in cases {
            let redis_plan = RedisEnqueuePlan::from_enqueue_plan(&plan, now).unwrap();
            let RedisEnqueueOperation::EvalScript(call) = &redis_plan.operations()[1] else {
                panic!("expected script call");
            };
            call.validate().unwrap();
        }
    }

    #[test]
    fn redis_complete_plans_match_script_shapes() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let mut messages = Vec::new();
        messages.push(active_message(0, ""));
        messages.push(active_message(
            0,
            "asynq:{critical}:unique:email:welcome:321c3cf486ed509164edec1e1981fec8",
        ));
        messages.push(active_message(300, ""));
        messages.push(active_message(
            300,
            "asynq:{critical}:unique:email:welcome:321c3cf486ed509164edec1e1981fec8",
        ));

        for message in messages {
            let redis_plan = RedisCompletePlan::from_message(&message, now).unwrap();
            redis_plan.call().validate().unwrap();
        }
    }

    #[test]
    fn redis_retry_plan_matches_script_shape() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let retry_at = now + Duration::from_secs(60);
        let message = active_message(0, "");

        let redis_plan =
            RedisRetryPlan::from_message(&message, now, retry_at, "handler failed", true).unwrap();

        redis_plan.call().validate().unwrap();
    }

    #[test]
    fn redis_archive_plan_matches_script_shape() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let message = active_message(0, "");

        let redis_plan =
            RedisArchivePlan::from_message(&message, now, now, "max retry exhausted", true)
                .unwrap();

        redis_plan.call().validate().unwrap();
    }

    #[test]
    fn redis_requeue_plan_matches_script_shape() {
        let message = active_message(0, "");

        let redis_plan = RedisRequeuePlan::from_message(&message).unwrap();

        redis_plan.call().validate().unwrap();
    }

    #[test]
    fn redis_forward_plans_match_script_shapes() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

        RedisForwardPlan::from_scheduled_queue("critical", now)
            .unwrap()
            .call()
            .validate()
            .unwrap();
        RedisForwardPlan::from_retry_queue("critical", now)
            .unwrap()
            .call()
            .validate()
            .unwrap();
    }

    #[test]
    fn redis_recover_plan_matches_script_shape() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

        RedisRecoverPlan::from_queue("critical", now)
            .unwrap()
            .call()
            .validate()
            .unwrap();
    }

    #[test]
    fn redis_extend_lease_plan_has_no_script_shape() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);

        let plan =
            RedisExtendLeasePlan::from_queue_and_task_id("critical", "task-id", now).unwrap();

        assert_eq!(plan.key(), "asynq:{critical}:lease");
        assert_eq!(plan.task_id(), "task-id");
        assert_eq!(plan.lease_expires_at_seconds(), 1_700_000_030);
    }

    fn active_message(retention: i64, unique_key: &str) -> TaskMessage {
        let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
        msg.id = "task-id".to_owned();
        msg.queue = "critical".to_owned();
        msg.retention = retention;
        msg.unique_key = unique_key.to_owned();
        msg
    }
}
