//! Redis aggregation set broker operations.
//!
//! Reference: Asynq v0.26.0 aggregation set Redis operations:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.

use std::time::{Duration, SystemTime};

use crate::broker::redis::{
    RedisAggregationCheckInput, RedisAggregationCheckPlan, RedisDeleteAggregationSetPlan,
    RedisExecutor, RedisReadAggregationSetPlan, RedisReclaimStaleAggregationSetsPlan,
    RedisScriptCall,
};
use crate::client::Clock;
use crate::task::TaskMessage;
use crate::{AggregationError, AggregationSet};

use super::parse::system_time_from_unix_seconds;
use super::{AggregationCheckRequest, RedisBroker, RedisBrokerError, redis_broker_error_message};

impl From<RedisBrokerError> for AggregationError {
    fn from(error: RedisBrokerError) -> Self {
        Self::Other(redis_broker_error_message(error))
    }
}

impl<E, C> RedisBroker<E, C>
where
    E: RedisExecutor,
    C: Clock,
{
    /// Reference: Asynq v0.26.0 `RDB.AggregationCheck` creates a set id and
    /// runs `aggregationCheckCmd` against a queue/group:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1270-L1286>.
    pub async fn aggregation_check(
        &mut self,
        queue: &str,
        group: &str,
        grace_period: Duration,
        max_delay: Duration,
        max_size: isize,
    ) -> Result<Option<String>, AggregationError> {
        self.aggregation_check_with_now(
            queue,
            group,
            self.clock.now(),
            grace_period,
            max_delay,
            max_size,
        )
        .await
    }

    pub async fn aggregation_check_with_now(
        &mut self,
        queue: &str,
        group: &str,
        now: SystemTime,
        grace_period: Duration,
        max_delay: Duration,
        max_size: isize,
    ) -> Result<Option<String>, AggregationError> {
        self.aggregation_check_with_request(AggregationCheckRequest {
            queue,
            group,
            set_id: uuid::Uuid::new_v4().to_string(),
            now,
            grace_period,
            max_delay,
            max_size,
        })
        .await
    }

    pub(in crate::broker::redis::broker) async fn aggregation_check_with_request(
        &mut self,
        request: AggregationCheckRequest<'_>,
    ) -> Result<Option<String>, AggregationError> {
        let redis_plan = RedisAggregationCheckPlan::from_group(RedisAggregationCheckInput {
            queue: request.queue,
            group: request.group,
            set_id: request.set_id,
            check_time: request.now,
            deadline_base: self.clock.now(),
            grace_period: request.grace_period,
            max_delay: request.max_delay,
            max_size: request.max_size,
        })
        .map_err(RedisBrokerError::AggregationCheckPlan)
        .map_err(AggregationError::from)?;
        let call = redis_plan.call();
        call.validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(AggregationError::from)?;
        let result = self
            .executor
            .eval_script_int(call)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(AggregationError::from)?;
        match result {
            0 => Ok(None),
            1 => Ok(Some(redis_plan.set_id().to_owned())),
            result => Err(AggregationError::Other(format!(
                "unexpected {:?} script result: {result}",
                call.script()
            ))),
        }
    }

    /// Reference: Asynq v0.26.0 `RDB.DeleteAggregationSet` runs
    /// `deleteAggregationSetCmd` for one queue/group/set id:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1343-L1353>.
    pub async fn delete_aggregation_set(
        &mut self,
        queue: &str,
        group: &str,
        set_id: &str,
    ) -> Result<(), AggregationError> {
        let redis_plan = RedisDeleteAggregationSetPlan::from_set(queue, group, set_id)
            .map_err(RedisBrokerError::DeleteAggregationSetPlan)
            .map_err(AggregationError::from)?;
        let call = redis_plan.call();
        self.eval_status_aggregation(call).await
    }

    /// Reference: Asynq v0.26.0 `RDB.ReadAggregationSet` reads the pending set
    /// payloads and uses the all-sets score as the deadline:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1325-L1341>.
    pub async fn read_aggregation_set(
        &mut self,
        queue: &str,
        group: &str,
        set_id: &str,
    ) -> Result<AggregationSet, AggregationError> {
        let redis_plan = RedisReadAggregationSetPlan::from_set(queue, group, set_id)
            .map_err(RedisBrokerError::ReadAggregationSetPlan)
            .map_err(AggregationError::from)?;
        let call = redis_plan.call();
        call.validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(AggregationError::from)?;
        let data = self
            .executor
            .eval_script_byte_vec(call)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(AggregationError::from)?;
        let mut messages = Vec::with_capacity(data.len());
        for item in data {
            messages.push(
                TaskMessage::decode_from_slice(&item)
                    .map_err(RedisBrokerError::Decode)
                    .map_err(AggregationError::from)?,
            );
        }
        let deadline_score = self
            .executor
            .zscore(
                redis_plan.all_aggregation_sets_key(),
                redis_plan.aggregation_set_key(),
            )
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(AggregationError::from)?;
        Ok(AggregationSet::new(
            messages,
            system_time_from_unix_seconds(deadline_score as i64),
        ))
    }

    /// Reference: Asynq v0.26.0 recoverer calls `RDB.ReclaimStaleAggregationSets`
    /// to move expired aggregation sets back into their group queues:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L785-L790>.
    pub async fn reclaim_stale_aggregation_sets(
        &mut self,
        queue: &str,
    ) -> Result<(), AggregationError> {
        self.reclaim_stale_aggregation_sets_with_now(queue, self.clock.now())
            .await
    }

    pub async fn reclaim_stale_aggregation_sets_with_now(
        &mut self,
        queue: &str,
        now: SystemTime,
    ) -> Result<(), AggregationError> {
        let redis_plan = RedisReclaimStaleAggregationSetsPlan::from_queue(queue, now)
            .map_err(RedisBrokerError::ReclaimStaleAggregationSetsPlan)
            .map_err(AggregationError::from)?;
        let call = redis_plan.call();
        self.eval_status_aggregation(call).await
    }

    async fn eval_status_aggregation(
        &mut self,
        call: &RedisScriptCall,
    ) -> Result<(), AggregationError> {
        call.validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(AggregationError::from)?;
        let status = self
            .executor
            .eval_script_status(call)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(AggregationError::from)?;
        if status == "OK" {
            Ok(())
        } else {
            Err(AggregationError::from(
                RedisBrokerError::UnexpectedScriptStatus {
                    script: call.script(),
                    status,
                },
            ))
        }
    }
}
