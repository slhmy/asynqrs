use std::collections::HashSet;

use crate::SystemClock;

#[derive(Debug, Clone)]
pub struct RedisBroker<E, C = SystemClock> {
    pub(in crate::broker::redis::broker) executor: E,
    pub(in crate::broker::redis::broker) clock: C,
    /// Reference: Asynq v0.26.0 `RDB.queuesPublished` avoids repeated
    /// `SADD asynq:queues` calls for queues already published by this broker:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L28-L35>.
    pub(in crate::broker::redis::broker) published_queues: HashSet<String>,
}

impl<E> RedisBroker<E, SystemClock> {
    pub fn new(executor: E) -> Self {
        Self::with_clock(executor, SystemClock)
    }
}

impl<E, C> RedisBroker<E, C> {
    pub fn with_clock(executor: E, clock: C) -> Self {
        Self {
            executor,
            clock,
            published_queues: HashSet::new(),
        }
    }

    pub fn executor(&self) -> &E {
        &self.executor
    }

    pub fn executor_mut(&mut self) -> &mut E {
        &mut self.executor
    }

    pub fn into_executor(self) -> E {
        self.executor
    }
}
