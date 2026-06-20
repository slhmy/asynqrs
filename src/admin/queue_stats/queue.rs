use std::time::{Duration, SystemTime};

use super::queue_latency::duration_nanos;

mod state_counts;
mod throughput_counts;

/// Current Redis-backed queue statistics.
///
/// Reference: Asynq v0.26.0 `Stats` inspector model:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueStats {
    pub(super) queue: String,
    pub(super) memory_usage: i64,
    pub(super) paused: bool,
    size: usize,
    groups: usize,
    pending: usize,
    active: usize,
    scheduled: usize,
    retry: usize,
    archived: usize,
    completed: usize,
    aggregating: usize,
    processed: usize,
    failed: usize,
    processed_total: usize,
    failed_total: usize,
    pub(super) latency: Duration,
    pub(super) latency_nanos: i128,
    pub(super) timestamp: SystemTime,
}

/// Public queue inspection model name matching upstream Asynq.
///
/// Reference: Asynq v0.26.0 public `QueueInfo` inspector model:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L89-L135>.
pub type QueueInfo = QueueStats;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueStateSnapshot {
    pub groups: usize,
    pub pending: usize,
    pub active: usize,
    pub scheduled: usize,
    pub retry: usize,
    pub archived: usize,
    pub completed: usize,
    pub aggregating: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueThroughputSnapshot {
    pub processed: usize,
    pub failed: usize,
    pub processed_total: usize,
    pub failed_total: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueStatsSnapshot {
    pub queue: String,
    pub memory_usage: i64,
    pub paused: bool,
    pub states: QueueStateSnapshot,
    pub throughput: QueueThroughputSnapshot,
    pub latency: Duration,
    pub timestamp: SystemTime,
}

impl QueueStats {
    pub fn new(snapshot: QueueStatsSnapshot) -> Self {
        let latency_nanos = duration_nanos(snapshot.latency);
        Self::with_signed_latency(snapshot, latency_nanos)
    }

    pub(crate) fn with_signed_latency(snapshot: QueueStatsSnapshot, latency_nanos: i128) -> Self {
        let states = snapshot.states;
        let throughput = snapshot.throughput;
        Self {
            queue: snapshot.queue,
            memory_usage: snapshot.memory_usage,
            paused: snapshot.paused,
            // Reference: Asynq v0.26.0 `RDB.CurrentStats` includes completed
            // tasks in the queue size while also exposing `Completed`
            // separately:
            // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L240-L266>.
            size: states.pending
                + states.active
                + states.scheduled
                + states.retry
                + states.archived
                + states.completed
                + states.aggregating,
            groups: states.groups,
            pending: states.pending,
            active: states.active,
            scheduled: states.scheduled,
            retry: states.retry,
            archived: states.archived,
            completed: states.completed,
            aggregating: states.aggregating,
            processed: throughput.processed,
            failed: throughput.failed,
            processed_total: throughput.processed_total,
            failed_total: throughput.failed_total,
            latency: snapshot.latency,
            latency_nanos,
            timestamp: snapshot.timestamp,
        }
    }
}
