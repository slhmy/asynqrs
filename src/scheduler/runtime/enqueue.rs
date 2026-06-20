use super::SchedulerRun;
use crate::client::Clock;
use crate::{EnqueuePlan, TaskInfo};

use crate::scheduler::SchedulerError;
use crate::scheduler::broker::SchedulerBroker;
use crate::scheduler::core::Scheduler;
use crate::scheduler::logging::{log_scheduler_debug, log_scheduler_warn};
use crate::scheduler::spec::unix_seconds;

impl<B, C> Scheduler<B, C>
where
    B: SchedulerBroker + Send,
    C: Clock + Send + Sync,
{
    pub(super) async fn run_due_entries(&mut self) -> Result<SchedulerRun, SchedulerError> {
        let now = self.clock.now();
        let mut enqueued = 0;

        for entry in &mut self.entries {
            if entry.next_enqueue_at > now {
                continue;
            }
            let plan = match EnqueuePlan::from_task_with_options(
                &entry.task,
                entry.options.clone(),
                now,
                format!("{}:{}", entry.id, unix_seconds(now)),
            ) {
                Ok(plan) => plan,
                Err(error) => {
                    // Reference: Asynq v0.26.0 scheduler jobs call
                    // `Client.Enqueue`; enqueue validation errors return only
                    // from the current cron job, so other due jobs keep
                    // running:
                    // <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L158-L165>.
                    if let Some(hook) = &self.enqueue_plan_error_hook {
                        hook(entry, &error);
                    }
                    entry.prev_enqueue_at = Some(now);
                    entry.next_enqueue_at = entry.schedule.next_after(now, &entry.spec)?;
                    continue;
                }
            };
            if let Some(hook) = &self.pre_enqueue_hook {
                hook(entry, &plan);
            }
            let enqueue_result = self.broker.enqueue_scheduled(&plan).await;
            if let Some(hook) = &self.post_enqueue_hook {
                hook(entry, &plan, enqueue_result.as_ref().map(|_| ()));
            }
            if let Err(error) = enqueue_result {
                // Reference: Asynq v0.26.0 calls the enqueue error handler
                // after post-enqueue hooks and then returns from only the
                // failed cron job; the scheduler keeps running other due
                // entries.
                // <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L420-L429>.
                if let Some(hook) = &self.enqueue_error_hook {
                    hook(entry, &plan, &error);
                }
                entry.prev_enqueue_at = Some(now);
                entry.next_enqueue_at = entry.schedule.next_after(now, &entry.spec)?;
                continue;
            }
            // Reference: Asynq v0.26.0 logs successfully enqueued scheduler
            // tasks before recording enqueue history:
            // <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L166-L170>.
            log_scheduler_debug(
                &self.logger,
                self.log_level,
                format_args!(
                    "scheduler enqueued a task: {:?}",
                    TaskInfo::from_enqueue_plan(&plan)
                ),
            );
            // Reference: Asynq v0.26.0 records scheduler enqueue history
            // after a successful enqueue, but only logs a warning when the
            // history write fails; the scheduler job itself has succeeded.
            // The event timestamp is a fresh `time.Now().In(location)` read,
            // separate from the cron tick time used for entry scheduling:
            // <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L167-L175>.
            let event_now = self.clock.now();
            if let Err(error) = self
                .broker
                .record_scheduler_enqueue_event(&entry.id, entry.event(&plan, event_now), event_now)
                .await
            {
                log_scheduler_warn(
                    &self.logger,
                    self.log_level,
                    format_args!(
                        "scheduler could not record enqueue event of enqueued task {}: {error}",
                        plan.message().id
                    ),
                );
            }
            entry.prev_enqueue_at = Some(now);
            entry.next_enqueue_at = entry.schedule.next_after(now, &entry.spec)?;
            enqueued += 1;
        }

        Ok(SchedulerRun { enqueued })
    }
}
