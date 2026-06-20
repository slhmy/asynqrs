use std::sync::Arc;

use crate::aggregation::{AggregationBroker, Aggregator};
use crate::{GroupAggregator, GroupAggregatorHandler};

use super::super::aggregation::{
    AggregationRunner, ServerAggregationConfig, ServerAggregatorRunner,
};
use super::super::{Server, ServerError, Sleeper};

impl<P, S> Server<P, S> {
    pub fn with_aggregation_config(mut self, config: ServerAggregationConfig) -> Self {
        self.aggregation_config = Some(config);
        self
    }

    pub fn with_aggregation_runner<R>(mut self, runner: R) -> Self
    where
        R: AggregationRunner + 'static,
    {
        self.aggregation_runner = Some(Arc::new(runner));
        self
    }

    /// Configures a server-owned aggregation runner from server group settings.
    ///
    /// Reference: Asynq v0.26.0 `NewServer` wires `Config.GroupAggregator`
    /// and group aggregation timing options into the server-owned aggregator:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L458-L465>.
    pub fn with_group_aggregator<B, G, R>(
        self,
        broker: B,
        group_aggregator: G,
        sleeper: R,
    ) -> Result<Self, ServerError>
    where
        B: AggregationBroker + Clone + Send + 'static,
        G: GroupAggregator + Send + 'static,
        R: Sleeper + Send + 'static,
    {
        let config = self
            .aggregation_config
            .ok_or(ServerError::MissingAggregationConfig)?;
        let mut aggregator = Aggregator::new(broker, GroupAggregatorHandler::new(group_aggregator))
            .with_log_level(self.log_level)
            .with_optional_logger(self.logger.clone());
        for group_config in self.aggregation_group_configs_for(config)? {
            aggregator.add_auto_group_config(group_config);
        }
        Ok(self.with_aggregation_runner(ServerAggregatorRunner::new(aggregator, sleeper)))
    }

    pub(in crate::server) fn aggregation_group_configs_for(
        &self,
        config: ServerAggregationConfig,
    ) -> Result<Vec<crate::AggregationGroupConfig>, ServerError> {
        self.queues
            .iter()
            .map(|queue| {
                config
                    .group_config(queue.clone())
                    .map_err(ServerError::from)
            })
            .collect()
    }
}
