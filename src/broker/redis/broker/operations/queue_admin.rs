use std::time::SystemTime;

use crate::broker::redis::keys;
use crate::broker::redis::{
    RedisDeleteQueuePlan, RedisExecutor, RedisListQueuesPlan, RedisPauseQueuePlan,
    RedisUnpauseQueuePlan,
};
use crate::client::Clock;
use crate::{AdminError, ClusterNode};

use super::{RedisBroker, RedisBrokerError};

impl<E, C> RedisBroker<E, C>
where
    E: RedisExecutor,
    C: Clock,
{
    /// Reference: Asynq v0.26.0 `Inspector.ClusterKeySlot` returns the Redis
    /// cluster hash slot for the queue pending key.
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L852-L854>.
    /// Reference: Asynq v0.26.0 Redis implementation delegates directly to
    /// Redis without a local empty queue-name precheck.
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L2085-L2089>.
    pub async fn cluster_key_slot(&mut self, queue: &str) -> Result<i64, AdminError> {
        self.executor
            .cluster_key_slot(&keys::pending_key(queue))
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(AdminError::from)
    }

    /// Reference: Asynq v0.26.0 `Inspector.ClusterNodes` returns the cluster
    /// nodes whose slot range contains the queue hash slot.
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L856-L877>.
    pub async fn cluster_nodes(&mut self, queue: &str) -> Result<Vec<ClusterNode>, AdminError> {
        let keyslot = self.cluster_key_slot(queue).await?;
        let slots = self
            .executor
            .cluster_slots()
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(AdminError::from)?;
        slots
            .into_iter()
            .find(|slot| slot.start() <= keyslot && keyslot <= slot.end())
            .map(|slot| slot.nodes().to_vec())
            .ok_or_else(|| AdminError::Other("nodes not found".to_owned()))
    }

    pub async fn pause_queue_with_now(
        &mut self,
        queue: &str,
        now: SystemTime,
    ) -> Result<(), AdminError> {
        let redis_plan = RedisPauseQueuePlan::from_queue(queue, now)
            .map_err(RedisBrokerError::AdminPlan)
            .map_err(AdminError::from)?;
        let inserted = self
            .executor
            .set_nx_i64(redis_plan.paused_key(), redis_plan.paused_at())
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(AdminError::from)?;
        if inserted {
            Ok(())
        } else {
            Err(AdminError::QueueAlreadyPaused)
        }
    }

    pub async fn pause_queue(&mut self, queue: &str) -> Result<(), AdminError> {
        self.pause_queue_with_now(queue, self.clock.now()).await
    }

    pub async fn unpause_queue(&mut self, queue: &str) -> Result<(), AdminError> {
        let redis_plan = RedisUnpauseQueuePlan::from_queue(queue)
            .map_err(RedisBrokerError::AdminPlan)
            .map_err(AdminError::from)?;
        let deleted = self
            .executor
            .del(redis_plan.paused_key())
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(AdminError::from)?;
        if deleted > 0 {
            Ok(())
        } else {
            Err(AdminError::QueueNotPaused)
        }
    }

    pub async fn delete_queue(&mut self, queue: &str) -> Result<(), AdminError> {
        self.delete_queue_with_force(queue, false).await
    }

    /// Reference: Asynq v0.26.0 `Inspector.DeleteQueue` can force-delete a
    /// non-active queue while preserving the active-task precondition.
    pub async fn delete_queue_force(&mut self, queue: &str) -> Result<(), AdminError> {
        self.delete_queue_with_force(queue, true).await
    }

    async fn delete_queue_with_force(
        &mut self,
        queue: &str,
        force: bool,
    ) -> Result<(), AdminError> {
        let redis_plan = RedisDeleteQueuePlan::from_queue_and_force(queue, force)
            .map_err(RedisBrokerError::AdminPlan)
            .map_err(AdminError::from)?;
        // Reference: Asynq v0.26.0 `RDB.RemoveQueue` checks queue
        // membership before running the Lua removal script:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1890-L1897>.
        self.ensure_queue_exists(queue).await?;
        let call = redis_plan.call();
        call.validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(AdminError::from)?;
        let result = self
            .executor
            .eval_script_int(call)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(AdminError::from)?;
        match result {
            1 => {
                // Reference: Asynq v0.26.0 `RDB.RemoveQueue` removes queue
                // membership only after its Lua removal succeeds:
                // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1854-L1909>.
                self.executor
                    .srem(redis_plan.queue_set_key(), redis_plan.queue())
                    .await
                    .map_err(RedisBrokerError::Executor)
                    .map_err(AdminError::from)?;
                Ok(())
            }
            -1 => Err(AdminError::QueueNotEmpty),
            -2 => Err(AdminError::QueueHasActiveTasks),
            result => Err(AdminError::Other(format!(
                "unexpected {:?} script result: {result}",
                call.script()
            ))),
        }
    }

    /// Reference: Asynq v0.26.0 `Inspector.Queues` lists all known queues:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L57-L59>.
    pub async fn list_queues(&mut self) -> Result<Vec<String>, AdminError> {
        let redis_plan = RedisListQueuesPlan::new();
        // Reference: Asynq v0.26.0 `Inspector.Queues` returns the Redis
        // `SMEMBERS` result from `RDB.ALL_QUEUES` without client-side sorting.
        // <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L57-L59>.
        self.executor
            .smembers(redis_plan.key())
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(AdminError::from)
    }
}
