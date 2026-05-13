use crate::{RedisArg, RedisEnqueueScript, RedisScriptCall};

/// Metadata and source for Asynq enqueue Lua scripts.
///
/// Reference: Asynq v0.26.0 enqueue-related Lua scripts:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L735>.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RedisScriptSpec {
    script: RedisEnqueueScript,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedisScriptCallError {
    WrongKeyCount {
        script: RedisEnqueueScript,
        expected: usize,
        actual: usize,
    },
    WrongArgCount {
        script: RedisEnqueueScript,
        expected: usize,
        actual: usize,
    },
}

impl RedisEnqueueScript {
    pub const ALL: [Self; 6] = [
        Self::Enqueue,
        Self::EnqueueUnique,
        Self::Schedule,
        Self::ScheduleUnique,
        Self::AddToGroup,
        Self::AddToGroupUnique,
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
        match code {
            1 => Some(RedisScriptResult::Success),
            0 => Some(RedisScriptResult::TaskIdConflict),
            -1 if self.supports_duplicate_result() => Some(RedisScriptResult::DuplicateTask),
            _ => None,
        }
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
    pub const fn script(self) -> RedisEnqueueScript {
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

impl std::fmt::Display for RedisScriptCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongKeyCount {
                script,
                expected,
                actual,
            } => write!(
                f,
                "{} script expected {expected} keys, got {actual}",
                script.name()
            ),
            Self::WrongArgCount {
                script,
                expected,
                actual,
            } => write!(
                f,
                "{} script expected {expected} args, got {actual}",
                script.name()
            ),
        }
    }
}

impl std::error::Error for RedisScriptCallError {}

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    use crate::{EnqueuePlan, RedisEnqueueOperation, RedisEnqueuePlan, Task, TaskOption};

    #[test]
    fn scripts_have_sources_and_shapes() {
        let expected = [
            (RedisEnqueueScript::Enqueue, "enqueue", 2, 3),
            (RedisEnqueueScript::EnqueueUnique, "enqueue_unique", 3, 4),
            (RedisEnqueueScript::Schedule, "schedule", 2, 3),
            (RedisEnqueueScript::ScheduleUnique, "schedule_unique", 3, 4),
            (RedisEnqueueScript::AddToGroup, "add_to_group", 3, 4),
            (
                RedisEnqueueScript::AddToGroupUnique,
                "add_to_group_unique",
                4,
                5,
            ),
        ];

        for (script, name, key_count, arg_count) in expected {
            let spec = script.spec();
            assert_eq!(spec.script(), script);
            assert_eq!(spec.name(), name);
            assert_eq!(spec.key_count(), key_count);
            assert_eq!(spec.arg_count(), arg_count);
            assert!(spec.source().contains("redis.call"));
            assert!(spec.source().contains("return 1"));
        }
    }

    #[test]
    fn scripts_map_return_codes() {
        assert_eq!(
            RedisEnqueueScript::Enqueue.result_for_code(1),
            Some(RedisScriptResult::Success)
        );
        assert_eq!(
            RedisEnqueueScript::Enqueue.result_for_code(0),
            Some(RedisScriptResult::TaskIdConflict)
        );
        assert_eq!(RedisEnqueueScript::Enqueue.result_for_code(-1), None);
        assert_eq!(
            RedisEnqueueScript::EnqueueUnique.result_for_code(-1),
            Some(RedisScriptResult::DuplicateTask)
        );
    }

    #[test]
    fn validates_script_call_shape() {
        let keys = vec!["k1".to_owned(), "k2".to_owned()];
        let args = vec![
            RedisArg::Bytes(Vec::new()),
            RedisArg::String("task-id".to_owned()),
            RedisArg::I64(1),
        ];

        assert_eq!(
            RedisEnqueueScript::Enqueue.validate_call(&keys, &args),
            Ok(())
        );
        assert_eq!(
            RedisEnqueueScript::Enqueue.validate_call(&keys[0..1], &args),
            Err(RedisScriptCallError::WrongKeyCount {
                script: RedisEnqueueScript::Enqueue,
                expected: 2,
                actual: 1,
            })
        );
        assert_eq!(
            RedisEnqueueScript::Enqueue.validate_call(&keys, &args[0..2]),
            Err(RedisScriptCallError::WrongArgCount {
                script: RedisEnqueueScript::Enqueue,
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
            let RedisEnqueueOperation::RunScript(call) = &redis_plan.operations()[1] else {
                panic!("expected script call");
            };
            call.validate().unwrap();
        }
    }
}
