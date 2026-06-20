use std::time::SystemTime;

use crate::broker::redis::keys;

use super::super::{
    DEFAULT_LEASE_DURATION, RedisArg, RedisExtendLeasePlanError, RedisRecoverPlanError,
    RedisScript, RedisScriptCall, unix_seconds_floor,
};

/// Redis command intent for listing active tasks whose leases have expired.
///
/// Reference: Asynq v0.26.0 recoverer lease-expired task listing:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisRecoverPlan {
    call: RedisScriptCall,
}

/// Redis command intent for extending an active task lease.
///
/// Reference: Asynq v0.26.0 `RDB.ExtendLease`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisExtendLeasePlan {
    key: String,
    task_ids: Vec<String>,
    lease_expires_at: SystemTime,
    lease_expires_at_seconds: i64,
}

impl RedisRecoverPlan {
    pub fn from_queue_and_cutoff(
        queue: &str,
        cutoff: SystemTime,
    ) -> Result<Self, RedisRecoverPlanError> {
        // Reference: Asynq v0.26.0 `RDB.ListLeaseExpired` receives the
        // recoverer-computed cutoff and passes it directly to its Redis
        // script:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1569-L1594>.

        Ok(Self {
            // Reference: Asynq v0.26.0 `RDB.ListLeaseExpired` derives the
            // lease key and task-key prefix directly from `qname`:
            // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1332-L1353>.
            call: RedisScriptCall::new(
                RedisScript::ListLeaseExpired,
                vec![keys::lease_key(queue)],
                vec![
                    RedisArg::I64(unix_seconds_recover(cutoff, "lease expiration scan")?),
                    RedisArg::String(keys::task_key_prefix(queue)),
                ],
            ),
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}

impl RedisExtendLeasePlan {
    pub fn from_queue_and_task_ids(
        queue: &str,
        task_ids: &[String],
        now: SystemTime,
    ) -> Result<Self, RedisExtendLeasePlanError> {
        let lease_expires_at = now.checked_add(DEFAULT_LEASE_DURATION).ok_or(
            RedisExtendLeasePlanError::TimeOverflow("lease extension expiration"),
        )?;
        let lease_expires_at_seconds =
            unix_seconds_extend_lease(lease_expires_at, "lease extension")?;

        // Reference: Asynq v0.26.0 `RDB.ExtendLease` derives the lease key
        // directly from `qname`:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1355-L1370>.
        // Reference: Asynq v0.26.0 `RDB.ExtendLease` writes the supplied task
        // ids directly as lease zset members:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1355-L1370>.
        Ok(Self {
            key: keys::lease_key(queue),
            task_ids: task_ids.to_vec(),
            lease_expires_at,
            lease_expires_at_seconds,
        })
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn task_ids(&self) -> &[String] {
        &self.task_ids
    }

    pub fn lease_expires_at(&self) -> SystemTime {
        self.lease_expires_at
    }

    pub fn lease_expires_at_seconds(&self) -> i64 {
        self.lease_expires_at_seconds
    }
}

fn unix_seconds_recover(
    time: SystemTime,
    context: &'static str,
) -> Result<i64, RedisRecoverPlanError> {
    let seconds = unix_seconds_floor(time);
    seconds
        .try_into()
        .map_err(|_| RedisRecoverPlanError::TimeOverflow(context))
}

fn unix_seconds_extend_lease(
    time: SystemTime,
    context: &'static str,
) -> Result<i64, RedisExtendLeasePlanError> {
    let seconds = unix_seconds_floor(time);
    seconds
        .try_into()
        .map_err(|_| RedisExtendLeasePlanError::TimeOverflow(context))
}
