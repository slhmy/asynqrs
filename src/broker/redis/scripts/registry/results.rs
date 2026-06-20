use crate::broker::redis::RedisScript;

use super::super::RedisScriptResult;

impl RedisScript {
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
}
