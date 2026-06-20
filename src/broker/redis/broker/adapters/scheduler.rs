use std::time::{Duration, SystemTime};

use crate::broker::redis::RedisExecutor;
use crate::client::Clock;
use crate::client::CloseBroker;
use crate::scheduler::SchedulerBroker;
use crate::{BrokerError, EnqueuePlan, MetadataError};

use super::super::RedisBroker;

#[async_trait::async_trait]
impl<E, C> SchedulerBroker for RedisBroker<E, C>
where
    E: RedisExecutor + Send,
    C: Clock + Send + Sync,
{
    async fn ping(&mut self) -> Result<(), String> {
        RedisBroker::ping(self)
            .await
            .map_err(|error| error.to_string())
    }

    fn close(&mut self) {
        let _ = CloseBroker::close(self);
    }

    async fn enqueue_scheduled(&mut self, plan: &EnqueuePlan) -> Result<(), BrokerError> {
        self.enqueue_with_now(plan, self.clock.now()).await
    }

    async fn write_scheduler_entries(
        &mut self,
        scheduler_id: &str,
        entries: Vec<(String, Vec<u8>)>,
        ttl: Duration,
    ) -> Result<(), MetadataError> {
        RedisBroker::write_scheduler_entries(self, scheduler_id, entries, ttl).await
    }

    async fn record_scheduler_enqueue_event(
        &mut self,
        entry_id: &str,
        event: Vec<u8>,
        now: SystemTime,
    ) -> Result<(), MetadataError> {
        RedisBroker::record_scheduler_enqueue_event(self, entry_id, event, now).await
    }

    async fn clear_scheduler_entries(&mut self, scheduler_id: &str) -> Result<(), MetadataError> {
        RedisBroker::clear_scheduler_entries(self, scheduler_id).await
    }

    async fn clear_scheduler_history(&mut self, entry_id: &str) -> Result<(), MetadataError> {
        RedisBroker::clear_scheduler_history(self, entry_id).await
    }
}
