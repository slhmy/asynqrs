/// Scheduler runtime state.
///
/// Reference: Asynq v0.26.0 scheduler tracks new, active, and closed states
/// to reject duplicate starts and restarts after shutdown:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L295-L308>.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SchedulerState {
    New,
    Active,
    Closed,
}
