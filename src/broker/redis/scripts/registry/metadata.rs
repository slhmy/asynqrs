use crate::broker::redis::RedisScript;

use super::super::sources::*;
use super::super::{RedisScriptArgShape, RedisScriptSpec};

pub(super) const fn spec(script: RedisScript) -> RedisScriptSpec {
    match script {
        RedisScript::WriteServerState => RedisScriptSpec::new(
            script,
            "write_server_state",
            WRITE_SERVER_STATE_SOURCE,
            2,
            RedisScriptArgShape::AtLeast(2),
        ),
        RedisScript::ClearServerState => RedisScriptSpec::new(
            script,
            "clear_server_state",
            CLEAR_SERVER_STATE_SOURCE,
            2,
            RedisScriptArgShape::Exact(0),
        ),
        RedisScript::ListServerKeys => RedisScriptSpec::new(
            script,
            "list_server_keys",
            LIST_SERVER_KEYS_SOURCE,
            1,
            RedisScriptArgShape::Exact(1),
        ),
        RedisScript::ListWorkerKeys => RedisScriptSpec::new(
            script,
            "list_worker_keys",
            LIST_WORKER_KEYS_SOURCE,
            1,
            RedisScriptArgShape::Exact(1),
        ),
        RedisScript::WriteSchedulerEntries => RedisScriptSpec::new(
            script,
            "write_scheduler_entries",
            WRITE_SCHEDULER_ENTRIES_SOURCE,
            1,
            RedisScriptArgShape::AtLeast(1),
        ),
        RedisScript::ListSchedulerEntries => RedisScriptSpec::new(
            script,
            "list_scheduler_entries",
            LIST_SCHEDULER_ENTRIES_SOURCE,
            1,
            RedisScriptArgShape::Exact(1),
        ),
        RedisScript::RecordSchedulerEnqueueEvent => RedisScriptSpec::new(
            script,
            "record_scheduler_enqueue_event",
            RECORD_SCHEDULER_ENQUEUE_EVENT_SOURCE,
            1,
            RedisScriptArgShape::Exact(3),
        ),
        _ => panic!("unsupported metadata script"),
    }
}
