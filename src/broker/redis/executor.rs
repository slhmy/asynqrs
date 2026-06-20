mod cluster;
mod connection;
mod traits;

use crate::ClusterNode;

pub use connection::RedisConnectionExecutor;
pub use traits::RedisExecutor;

/// Redis cluster slot range returned by `CLUSTER SLOTS`.
///
/// Reference: Asynq v0.26.0 `RDB.ClusterNodes` scans go-redis
/// `ClusterSlot` ranges and returns nodes for the queue's hash slot:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L2091-L2107>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisSlotRange {
    start: i64,
    end: i64,
    nodes: Vec<ClusterNode>,
}

impl RedisSlotRange {
    pub fn new(start: i64, end: i64, nodes: Vec<ClusterNode>) -> Self {
        Self { start, end, nodes }
    }

    pub fn start(&self) -> i64 {
        self.start
    }

    pub fn end(&self) -> i64 {
        self.end
    }

    pub fn nodes(&self) -> &[ClusterNode] {
        &self.nodes
    }
}
