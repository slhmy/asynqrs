use crate::broker::redis::RedisScript;

use super::super::sources::*;
use super::super::{RedisScriptArgShape, RedisScriptSpec};

pub(super) const fn spec(script: RedisScript) -> RedisScriptSpec {
    match script {
        RedisScript::Dequeue => RedisScriptSpec::new(
            script,
            "dequeue",
            DEQUEUE_SOURCE,
            4,
            RedisScriptArgShape::Exact(2),
        ),
        RedisScript::Done => RedisScriptSpec::new(
            script,
            "done",
            DONE_SOURCE,
            5,
            RedisScriptArgShape::Exact(3),
        ),
        RedisScript::DoneUnique => RedisScriptSpec::new(
            script,
            "done_unique",
            DONE_UNIQUE_SOURCE,
            6,
            RedisScriptArgShape::Exact(3),
        ),
        RedisScript::MarkAsComplete => RedisScriptSpec::new(
            script,
            "mark_as_complete",
            MARK_AS_COMPLETE_SOURCE,
            6,
            RedisScriptArgShape::Exact(5),
        ),
        RedisScript::MarkAsCompleteUnique => RedisScriptSpec::new(
            script,
            "mark_as_complete_unique",
            MARK_AS_COMPLETE_UNIQUE_SOURCE,
            7,
            RedisScriptArgShape::Exact(5),
        ),
        RedisScript::Retry => RedisScriptSpec::new(
            script,
            "retry",
            RETRY_SOURCE,
            8,
            RedisScriptArgShape::Exact(6),
        ),
        RedisScript::Archive => RedisScriptSpec::new(
            script,
            "archive",
            ARCHIVE_SOURCE,
            9,
            RedisScriptArgShape::Exact(7),
        ),
        RedisScript::Requeue => RedisScriptSpec::new(
            script,
            "requeue",
            REQUEUE_SOURCE,
            4,
            RedisScriptArgShape::Exact(1),
        ),
        RedisScript::Forward => RedisScriptSpec::new(
            script,
            "forward",
            FORWARD_SOURCE,
            2,
            RedisScriptArgShape::Exact(4),
        ),
        RedisScript::DeleteExpiredCompletedTasks => RedisScriptSpec::new(
            script,
            "delete_expired_completed_tasks",
            DELETE_EXPIRED_COMPLETED_TASKS_SOURCE,
            1,
            RedisScriptArgShape::Exact(3),
        ),
        RedisScript::ListLeaseExpired => RedisScriptSpec::new(
            script,
            "list_lease_expired",
            LIST_LEASE_EXPIRED_SOURCE,
            1,
            RedisScriptArgShape::Exact(2),
        ),
        _ => panic!("unsupported lifecycle script"),
    }
}
