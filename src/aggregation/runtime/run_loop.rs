use std::sync::Arc;

use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;

use super::super::MAX_CONCURRENT_AGGREGATION_CHECKS;
use super::super::broker::AggregationBroker;
use super::super::handler::AggregationHandler;
use super::super::{AggregationError, Aggregator, AggregatorRun};
use super::logging::{log_aggregator_debug, log_aggregator_warn};
use super::run_once_with_broker;
use crate::client::Clock;
use crate::server::Sleeper;

impl<B, H, C> Aggregator<B, H, C>
where
    B: AggregationBroker + Send,
    H: AggregationHandler + Send,
    C: Clock + Send + Sync,
{
    pub(crate) async fn run_until_stopped<S>(
        &mut self,
        sleeper: &mut S,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<AggregatorRun, AggregationError>
    where
        S: Sleeper + Send,
        B: Clone,
        B: 'static,
        H: 'static,
    {
        let mut summary = AggregatorRun::default();
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_AGGREGATION_CHECKS));
        let mut checks = JoinSet::new();
        while !*shutdown.borrow() {
            merge_finished_checks(&mut checks, &mut summary)?;
            // Reference: Asynq v0.26.0 aggregator starts a `time.Ticker` and
            // only runs aggregation from ticker events, so startup does not
            // perform an immediate aggregation pass:
            // <https://github.com/hibiken/asynq/blob/v0.26.0/aggregator.go#L61-L83>.
            tokio::select! {
                _ = sleeper.sleep(self.tick_interval) => {
                    if *shutdown.borrow() {
                        break;
                    }
                }
                result = checks.join_next(), if !checks.is_empty() => {
                    merge_join_result(result, &mut summary)?;
                    continue;
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                    continue;
                }
            }
            // Reference: Asynq v0.26.0 aggregator `exec` acquires from a
            // fixed-size semaphore, starts a check goroutine when capacity is
            // available, and skips the tick when all slots are in use:
            // <https://github.com/hibiken/asynq/blob/v0.26.0/aggregator.go#L85-L96>.
            match Arc::clone(&semaphore).try_acquire_owned() {
                Ok(permit) => {
                    let mut broker = self.broker.clone();
                    let handler = Arc::clone(&self.handler);
                    let groups = self.groups.clone();
                    let auto_groups = self.auto_groups.clone();
                    let now = self.clock.now();
                    let logger = self.logger.clone();
                    let log_level = self.log_level;
                    checks.spawn(async move {
                        let _permit = permit;
                        run_once_with_broker(
                            &mut broker,
                            handler,
                            groups,
                            auto_groups,
                            now,
                            &logger,
                            log_level,
                        )
                        .await
                    });
                }
                Err(_) => {
                    log_aggregator_warn(
                        &self.logger,
                        self.log_level,
                        format_args!("Max number of aggregation checks in flight. Skipping"),
                    );
                    summary.skipped += 1;
                }
            }
        }
        // Reference: Asynq v0.26.0 aggregator waits for all in-flight
        // aggregation checks during shutdown and logs before/after waiting:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/aggregator.go#L69-L79>.
        log_aggregator_debug(
            &self.logger,
            self.log_level,
            format_args!("Waiting for all aggregation checks to finish..."),
        );
        while let Some(result) = checks.join_next().await {
            merge_join_result(Some(result), &mut summary)?;
        }
        log_aggregator_debug(
            &self.logger,
            self.log_level,
            format_args!("Aggregator done"),
        );
        Ok(summary)
    }
}

fn merge_finished_checks(
    checks: &mut JoinSet<Result<AggregatorRun, AggregationError>>,
    summary: &mut AggregatorRun,
) -> Result<(), AggregationError> {
    while let Some(result) = checks.try_join_next() {
        merge_join_result(Some(result), summary)?;
    }
    Ok(())
}

fn merge_join_result(
    result: Option<Result<Result<AggregatorRun, AggregationError>, tokio::task::JoinError>>,
    summary: &mut AggregatorRun,
) -> Result<(), AggregationError> {
    let Some(result) = result else {
        return Ok(());
    };
    let run = result.map_err(|error| AggregationError::Other(error.to_string()))??;
    summary.merge(run);
    Ok(())
}
