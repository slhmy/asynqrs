use super::super::QueueStats;

impl QueueStats {
    pub fn size(&self) -> usize {
        self.size
    }

    /// Returns mutable access to the total queue size.
    ///
    /// Reference: Asynq v0.26.0 public `QueueInfo.Size` is an exported int
    /// field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L101-L103>.
    pub fn size_mut(&mut self) -> &mut usize {
        &mut self.size
    }
    pub fn groups(&self) -> usize {
        self.groups
    }

    /// Returns mutable access to the aggregating group count.
    ///
    /// Reference: Asynq v0.26.0 public `QueueInfo.Groups` is an exported int
    /// field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L105-L106>.
    pub fn groups_mut(&mut self) -> &mut usize {
        &mut self.groups
    }
}
