use super::runtime::run_once_with_broker;
use super::*;
use crate::client::Clock;
use crate::server::{Sleeper, TokioSleeper};
use crate::task::TaskMessage;
use crate::{BrokerError, EnqueuePlan, Logger, Task};
use async_trait::async_trait;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::SystemTime;
use tokio::sync::watch;

mod config;
mod fixtures;
mod handler;
mod run_once;
mod runtime;

use fixtures::*;

trait AggregatorTestExt: Sized {
    fn with_test_logger(self, logger: Arc<dyn Logger>) -> Self;
    fn with_test_tick_interval(self, tick_interval: Duration) -> Self;
    fn add_test_group(&mut self, group: AggregationGroup);
}

impl<B, H, C> AggregatorTestExt for Aggregator<B, H, C> {
    fn with_test_logger(mut self, logger: Arc<dyn Logger>) -> Self {
        self.logger = Some(logger);
        self
    }

    fn with_test_tick_interval(mut self, tick_interval: Duration) -> Self {
        self.tick_interval = tick_interval;
        self
    }

    fn add_test_group(&mut self, group: AggregationGroup) {
        self.tick_interval = self.tick_interval.min(group.grace_period);
        self.groups.push(group);
    }
}

async fn run_aggregator_once<B, H, C>(
    aggregator: &mut Aggregator<B, H, C>,
) -> Result<AggregatorRun, AggregationError>
where
    B: AggregationBroker + Send,
    H: AggregationHandler + Send,
    C: Clock + Send + Sync,
{
    let now = aggregator.clock.now();
    run_once_with_broker(
        &mut aggregator.broker,
        Arc::clone(&aggregator.handler),
        aggregator.groups.clone(),
        aggregator.auto_groups.clone(),
        now,
        &aggregator.logger,
        aggregator.log_level,
    )
    .await
}
