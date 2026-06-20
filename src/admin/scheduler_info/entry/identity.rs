use super::SchedulerEntryInfo;

impl SchedulerEntryInfo {
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns mutable access to the scheduler entry ID.
    ///
    /// Reference: Asynq v0.26.0 public `SchedulerEntry.ID` is an exported
    /// string field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L901-L922>.
    pub fn id_mut(&mut self) -> &mut String {
        &mut self.id
    }
    pub fn spec(&self) -> &str {
        &self.spec
    }

    /// Returns mutable access to the scheduler entry cron/every spec.
    ///
    /// Reference: Asynq v0.26.0 public `SchedulerEntry.Spec` is an exported
    /// string field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L901-L922>.
    pub fn spec_mut(&mut self) -> &mut String {
        &mut self.spec
    }
}
