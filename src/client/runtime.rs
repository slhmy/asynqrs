use super::{Client, ClientEnqueueScope, ClientError, Clock, TaskIdGenerator};
use crate::{EnqueueOptions, EnqueuePlan, EnqueuePlanError, Task};

mod async_runtime;
mod close;
mod sync;

impl<B, G, C> Client<B, G, C>
where
    G: TaskIdGenerator,
    C: Clock,
{
    fn enqueue_plan(
        &mut self,
        task: &Task,
        options: EnqueueOptions,
    ) -> Result<EnqueuePlan, ClientError> {
        // Reference: Asynq v0.26.0 `Client.EnqueueContext` validates the task
        // type before calling `composeOptions`, which generates the default
        // task id and reads option clocks:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L299-L307>.
        if task.type_name().trim().is_empty() {
            return Err(ClientError::Plan(EnqueuePlanError::EmptyTaskType));
        }

        // Reference: Asynq v0.26.0 `composeOptions` generates the default
        // task id before taking the `processAt`/`ProcessIn` clock reading:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L217-L264>.
        let generated_task_id = self.task_id_generator.generate_task_id();
        EnqueuePlan::from_task_with_options_using_clocks(
            task,
            options,
            || self.clock.now(),
            || self.clock.now(),
            || self.clock.now(),
            generated_task_id,
        )
        .map_err(ClientError::Plan)
    }

    fn check_enqueue_scoped(scope: &ClientEnqueueScope) -> Result<(), ClientError> {
        // Reference: Asynq v0.26.0 `Client.EnqueueContext` passes the caller
        // context into the selected enqueue/schedule/group broker operation:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L314-L331>.
        if scope.is_cancelled() {
            return Err(ClientError::Cancelled);
        }
        Ok(())
    }
}
