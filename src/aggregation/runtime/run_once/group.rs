use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::Mutex;

use super::super::super::broker::AggregationBroker;
use super::super::super::handler::AggregationHandler;
use super::super::super::{AggregationError, AggregationGroup, AggregatorRun};
use super::super::logging::{log_aggregator_debug, log_aggregator_error, log_aggregator_warn};
use crate::server::{LogLevel, Logger};
use crate::{EnqueueOptions, EnqueuePlan};

pub(super) async fn run_aggregation_group<B, H>(
    broker: &mut B,
    handler: &Arc<Mutex<H>>,
    group: &AggregationGroup,
    now: SystemTime,
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
) -> Result<AggregatorRun, AggregationError>
where
    B: AggregationBroker + Send,
    H: AggregationHandler + Send,
{
    let mut summary = AggregatorRun::default();
    // Reference: Asynq v0.26.0 aggregator logs aggregation-check failures and
    // continues with later groups.
    // <https://github.com/hibiken/asynq/blob/v0.26.0/aggregator.go#L111-L116>.
    let set_id = match broker
        .aggregation_check(
            &group.queue,
            &group.group,
            now,
            group.grace_period,
            group.max_delay,
            group.max_size,
        )
        .await
    {
        Ok(set_id) => set_id,
        Err(_error) => {
            log_aggregator_error(
                logger,
                log_level,
                format_args!(
                    "Failed to run aggregation check: queue={:?} group={:?}",
                    group.queue, group.group
                ),
            );
            return Ok(summary);
        }
    };
    let Some(set_id) = set_id else {
        log_aggregator_debug(
            logger,
            log_level,
            format_args!(
                "No aggregation needed at this time: queue={:?} group={:?}",
                group.queue, group.group
            ),
        );
        summary.checked += 1;
        return Ok(summary);
    };

    // Reference: Asynq v0.26.0 aggregator logs read failures for a ready
    // aggregation set and continues with later groups.
    // <https://github.com/hibiken/asynq/blob/v0.26.0/aggregator.go#L119-L123>.
    let set = match broker
        .read_aggregation_set(&group.queue, &group.group, &set_id)
        .await
    {
        Ok(set) => set,
        Err(_error) => {
            log_aggregator_error(
                logger,
                log_level,
                format_args!(
                    "Failed to read aggregation set: queue={:?}, group={:?}, setID={:?}",
                    group.queue, group.group, set_id
                ),
            );
            return Ok(summary);
        }
    };
    let deadline = set.deadline();
    let aggregated_task = match handler
        .lock()
        .await
        .handle_aggregation(&group.queue, &group.group, &set_id, set)
        .await
    {
        Ok(task) => task,
        Err(error) => {
            // Reference: Asynq v0.26.0 aggregator logs per-set aggregation
            // failures and continues without enqueueing or deleting the
            // source aggregation set:
            // <https://github.com/hibiken/asynq/blob/v0.26.0/aggregator.go#L124-L132>.
            log_aggregator_error(
                logger,
                log_level,
                format_args!(
                    "Failed to aggregate task (queue={:?}, group={:?}, setID={:?}): {error}",
                    group.queue, group.group, set_id
                ),
            );
            return Ok(summary);
        }
    };
    // Reference: Asynq v0.26.0 aggregator calls the user aggregator before
    // enqueueing with `context.WithDeadline(context.Background(), deadline)`;
    // an already expired aggregation-set deadline prevents enqueue and leaves
    // the source set in place.
    // <https://github.com/hibiken/asynq/blob/v0.26.0/aggregator.go#L119-L133>.
    if deadline <= now {
        log_aggregator_error(
            logger,
            log_level,
            format_args!(
                "Failed to enqueue aggregated task (queue={:?}, group={:?}, setID={:?}): context deadline exceeded",
                group.queue, group.group, set_id
            ),
        );
        return Ok(summary);
    }
    let plan = match EnqueuePlan::from_task_with_options(
        &aggregated_task,
        EnqueueOptions::new().queue(crate::QueueName::new(group.queue.clone()).unwrap()),
        now,
        uuid::Uuid::new_v4().to_string(),
    ) {
        Ok(plan) => plan,
        Err(error) => {
            // Reference: Asynq v0.26.0 aggregator calls
            // `Client.EnqueueContext`; enqueue validation errors are logged
            // for the current aggregation set and the source set is left in
            // place:
            // <https://github.com/hibiken/asynq/blob/v0.26.0/aggregator.go#L127-L132>.
            log_aggregator_error(
                logger,
                log_level,
                format_args!(
                    "Failed to enqueue aggregated task (queue={:?}, group={:?}, setID={:?}): {error}",
                    group.queue, group.group, set_id
                ),
            );
            return Ok(summary);
        }
    };
    // Reference: Asynq v0.26.0 aggregator logs aggregate enqueue failures and
    // continues without deleting the source aggregation set.
    // <https://github.com/hibiken/asynq/blob/v0.26.0/aggregator.go#L129-L132>.
    if let Err(error) = broker.enqueue_aggregated(&plan).await {
        log_aggregator_error(
            logger,
            log_level,
            format_args!(
                "Failed to enqueue aggregated task (queue={:?}, group={:?}, setID={:?}): {error}",
                group.queue, group.group, set_id
            ),
        );
        return Ok(summary);
    }
    // Reference: Asynq v0.26.0 aggregator logs delete failures after a
    // successful aggregate enqueue and continues instead of failing the
    // aggregation run.
    // <https://github.com/hibiken/asynq/blob/v0.26.0/aggregator.go#L134-L137>.
    if let Err(_error) = broker
        .delete_aggregation_set(&group.queue, &group.group, &set_id)
        .await
    {
        log_aggregator_warn(
            logger,
            log_level,
            format_args!(
                "Failed to delete aggregation set: queue={:?}, group={:?}, setID={:?}",
                group.queue, group.group, set_id
            ),
        );
    }

    summary.checked += 1;
    summary.aggregated += 1;
    Ok(summary)
}
