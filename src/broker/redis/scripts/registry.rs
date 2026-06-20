mod admin;
mod aggregation;
mod all;
mod enqueue;
mod lifecycle;
mod metadata;
mod results;
mod specs;

use crate::broker::redis::RedisScript;

impl RedisScript {
    pub const fn name(self) -> &'static str {
        self.spec().name()
    }

    pub const fn source(self) -> &'static str {
        self.spec().source()
    }

    pub const fn key_count(self) -> usize {
        self.spec().key_count()
    }

    pub const fn arg_count(self) -> usize {
        self.spec().min_arg_count()
    }
}
