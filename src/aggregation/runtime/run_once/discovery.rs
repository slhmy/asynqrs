use std::sync::Arc;
use std::time::SystemTime;

use super::super::super::broker::AggregationBroker;
use super::super::super::{AggregationError, AggregationGroup, AggregationGroupConfig};
use super::super::logging::{log_aggregator_error, log_aggregator_warn};
use crate::server::{LogLevel, Logger};

pub(super) struct DiscoveredAggregationGroups {
    pub(super) groups: Vec<AggregationGroup>,
    pub(super) reclaimed: usize,
}

pub(super) async fn discover_groups<B>(
    broker: &mut B,
    configured_groups: Vec<AggregationGroup>,
    auto_groups: &[AggregationGroupConfig],
    now: SystemTime,
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
) -> Result<DiscoveredAggregationGroups, AggregationError>
where
    B: AggregationBroker + Send,
{
    let mut groups = configured_groups;
    let mut reclaimed = 0;
    for config in auto_groups {
        if reclaim_stale_aggregation_sets(broker, &config.queue, now, logger, log_level).await {
            reclaimed += 1;
        }
        // Reference: Asynq v0.26.0 aggregator logs group-list failures for a
        // queue and continues with later queues.
        // <https://github.com/hibiken/asynq/blob/v0.26.0/aggregator.go#L104-L109>.
        let discovered_groups = match broker.list_aggregation_groups(&config.queue).await {
            Ok(groups) => groups,
            Err(_error) => {
                log_aggregator_error(
                    logger,
                    log_level,
                    format_args!("Failed to list groups in queue: {:?}", config.queue),
                );
                continue;
            }
        };
        for group in discovered_groups {
            groups.push(config.group(group)?);
        }
    }

    Ok(DiscoveredAggregationGroups { groups, reclaimed })
}

pub(super) async fn reclaim_stale_aggregation_sets<B>(
    broker: &mut B,
    queue: &str,
    now: SystemTime,
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
) -> bool
where
    B: AggregationBroker + Send,
{
    // Reference: Asynq v0.26.0 recoverer logs stale aggregation-set reclaim
    // failures per queue and continues with later recovery work.
    // <https://github.com/hibiken/asynq/blob/v0.26.0/recoverer.go#L88-L94>.
    match broker.reclaim_stale_aggregation_sets(queue, now).await {
        Ok(()) => true,
        Err(error) => {
            log_aggregator_warn(
                logger,
                log_level,
                format_args!(
                    "recoverer: could not reclaim stale aggregation sets in queue {queue:?}: {error}"
                ),
            );
            false
        }
    }
}
