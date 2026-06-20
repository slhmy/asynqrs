use crate::broker::redis::RedisScript;

use super::super::sources::*;
use super::super::{RedisScriptArgShape, RedisScriptSpec};

pub(super) const fn spec(script: RedisScript) -> RedisScriptSpec {
    match script {
        RedisScript::Enqueue => RedisScriptSpec::new(
            script,
            "enqueue",
            ENQUEUE_SOURCE,
            2,
            RedisScriptArgShape::Exact(3),
        ),
        RedisScript::EnqueueUnique => RedisScriptSpec::new(
            script,
            "enqueue_unique",
            ENQUEUE_UNIQUE_SOURCE,
            3,
            RedisScriptArgShape::Exact(4),
        ),
        RedisScript::Schedule => RedisScriptSpec::new(
            script,
            "schedule",
            SCHEDULE_SOURCE,
            2,
            RedisScriptArgShape::Exact(3),
        ),
        RedisScript::ScheduleUnique => RedisScriptSpec::new(
            script,
            "schedule_unique",
            SCHEDULE_UNIQUE_SOURCE,
            3,
            RedisScriptArgShape::Exact(4),
        ),
        RedisScript::AddToGroup => RedisScriptSpec::new(
            script,
            "add_to_group",
            ADD_TO_GROUP_SOURCE,
            3,
            RedisScriptArgShape::Exact(4),
        ),
        RedisScript::AddToGroupUnique => RedisScriptSpec::new(
            script,
            "add_to_group_unique",
            ADD_TO_GROUP_UNIQUE_SOURCE,
            4,
            RedisScriptArgShape::Exact(5),
        ),
        _ => panic!("unsupported enqueue script"),
    }
}
