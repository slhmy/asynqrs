use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::Mutex;

mod discovery;
mod group;

use discovery::{discover_groups, reclaim_stale_aggregation_sets};
use group::run_aggregation_group;

use super::super::broker::AggregationBroker;
use super::super::handler::AggregationHandler;
use super::super::{AggregationError, AggregationGroup, AggregationGroupConfig, AggregatorRun};
use crate::server::{LogLevel, Logger};

pub(in crate::aggregation) async fn run_once_with_broker<B, H>(
    broker: &mut B,
    handler: Arc<Mutex<H>>,
    configured_groups: Vec<AggregationGroup>,
    auto_groups: Vec<AggregationGroupConfig>,
    now: SystemTime,
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
) -> Result<AggregatorRun, AggregationError>
where
    B: AggregationBroker + Send,
    H: AggregationHandler + Send,
{
    let mut summary = AggregatorRun::default();
    let groups = discover_groups(
        broker,
        configured_groups,
        &auto_groups,
        now,
        logger,
        log_level,
    )
    .await?;
    summary.reclaimed += groups.reclaimed;

    for group in &groups.groups {
        if auto_groups.iter().all(|config| config.queue != group.queue)
            && reclaim_stale_aggregation_sets(broker, &group.queue, now, logger, log_level).await
        {
            summary.reclaimed += 1;
        }

        summary
            .merge(run_aggregation_group(broker, &handler, group, now, logger, log_level).await?);
    }

    Ok(summary)
}
