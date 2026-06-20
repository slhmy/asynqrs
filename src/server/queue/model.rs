/// Queue name and priority weight used by server dequeue selection.
///
/// Reference: Asynq v0.26.0 `Config.Queues` maps queue names to priority
/// weights:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L151-L170>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueConfig {
    pub(super) name: String,
    pub(super) priority: usize,
}

impl QueueConfig {
    pub fn new(name: impl Into<String>, priority: usize) -> Self {
        Self {
            name: name.into(),
            priority,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn priority(&self) -> usize {
        self.priority
    }
}
