use std::collections::HashMap;

use super::ServerInfo;

impl ServerInfo {
    pub fn concurrency(&self) -> i32 {
        self.concurrency
    }

    /// Returns mutable access to the server concurrency.
    ///
    /// Reference: Asynq v0.26.0 public `ServerInfo.Concurrency` is an exported
    /// int field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L794-L822>.
    pub fn concurrency_mut(&mut self) -> &mut i32 {
        &mut self.concurrency
    }
    pub fn queues(&self) -> &HashMap<String, i32> {
        &self.queues
    }

    /// Returns mutable access to the server queue priority map.
    ///
    /// Reference: Asynq v0.26.0 public `ServerInfo.Queues` is an exported map
    /// field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L794-L822>.
    pub fn queues_mut(&mut self) -> &mut HashMap<String, i32> {
        &mut self.queues
    }
    pub fn strict_priority(&self) -> bool {
        self.strict_priority
    }

    /// Returns mutable access to the strict-priority flag.
    ///
    /// Reference: Asynq v0.26.0 public `ServerInfo.StrictPriority` is an
    /// exported bool field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L794-L822>.
    pub fn strict_priority_mut(&mut self) -> &mut bool {
        &mut self.strict_priority
    }
}
