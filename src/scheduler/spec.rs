pub(in crate::scheduler) mod duration;
mod schedule;
pub(in crate::scheduler) mod time;

use crate::EnqueueOptions;

pub(super) use duration::format_duration_spec;
pub(super) use schedule::{SchedulerEntrySchedule, parse_scheduler_spec};
pub(super) use time::unix_seconds;

pub(in crate::scheduler) fn task_options_metadata(options: &EnqueueOptions) -> Vec<String> {
    // Reference: Asynq v0.26.0 `SchedulerEntry.EnqueueOptions` stores the
    // string form of the `Register` options attached to the scheduler job via
    // `stringifyOptions(job.opts)`, not options already embedded in `Task`:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L76-L84>.
    options.scheduler_metadata()
}

pub(in crate::scheduler) fn generated_scheduler_entry_id() -> String {
    // Reference: Asynq v0.26.0 generates scheduler entry ids with
    // `uuid.New()` when registering a job:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L201-L225>.
    uuid::Uuid::new_v4().to_string()
}

pub(in crate::scheduler) fn generated_scheduler_id() -> String {
    // Reference: Asynq v0.26.0 generates scheduler ids as
    // `hostname:pid:uuid`, falling back to `unknown-host` when hostname lookup
    // fails:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L105-L111>.
    let hostname = hostname::get()
        .ok()
        .and_then(|name| name.into_string().ok())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "unknown-host".to_owned());
    format!(
        "{}:{}:{}",
        hostname,
        std::process::id(),
        uuid::Uuid::new_v4()
    )
}
