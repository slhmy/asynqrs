/// Reference: Asynq v0.26.0 `GroupStat` inspector model:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupStats {
    group: String,
    size: usize,
}

/// Public group inspection model name matching upstream Asynq.
///
/// Reference: Asynq v0.26.0 public `GroupInfo` inspector model:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L78-L87>.
pub type GroupInfo = GroupStats;

impl GroupStats {
    pub fn new(group: String, size: usize) -> Self {
        Self { group, size }
    }

    pub fn group(&self) -> &str {
        &self.group
    }

    /// Returns mutable access to the group name.
    ///
    /// Reference: Asynq v0.26.0 public `GroupInfo.Group` is an exported
    /// string field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L78-L87>.
    pub fn group_mut(&mut self) -> &mut String {
        &mut self.group
    }
    pub fn size(&self) -> usize {
        self.size
    }

    /// Returns mutable access to the group size.
    ///
    /// Reference: Asynq v0.26.0 public `GroupInfo.Size` is an exported int
    /// field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L78-L87>.
    pub fn size_mut(&mut self) -> &mut usize {
        &mut self.size
    }
}
