use crate::broker::redis::RedisScript;

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
    arg_shape: RedisScriptArgShape,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisScriptArgShape {
    Exact(usize),
    AtLeast(usize),
    OddAtLeast(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisScriptResult {
    Success,
    TaskIdConflict,
    DuplicateTask,
}

impl RedisScriptSpec {
    pub(super) const fn new(
        script: RedisScript,
        name: &'static str,
        source: &'static str,
        key_count: usize,
        arg_shape: RedisScriptArgShape,
    ) -> Self {
        Self {
            script,
            name,
            source,
            key_count,
            arg_shape,
        }
    }

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
        self.min_arg_count()
    }

    pub const fn arg_shape(self) -> RedisScriptArgShape {
        self.arg_shape
    }

    pub const fn min_arg_count(self) -> usize {
        match self.arg_shape {
            RedisScriptArgShape::Exact(count)
            | RedisScriptArgShape::AtLeast(count)
            | RedisScriptArgShape::OddAtLeast(count) => count,
        }
    }

    pub const fn matches_arg_count(self, count: usize) -> bool {
        match self.arg_shape {
            RedisScriptArgShape::Exact(expected) => count == expected,
            RedisScriptArgShape::AtLeast(min) => count >= min,
            RedisScriptArgShape::OddAtLeast(min) => count >= min && count % 2 == 1,
        }
    }
}
