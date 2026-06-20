//! Admin-surface error mapping for Redis broker internals.

use crate::AdminError;
use crate::broker::redis::RedisAdminPlanError;

use super::{RedisBrokerError, redis_broker_error_message};

impl From<RedisBrokerError> for AdminError {
    fn from(error: RedisBrokerError) -> Self {
        match error {
            RedisBrokerError::AdminPlan(RedisAdminPlanError::NonPositiveDays) => {
                Self::NonPositiveDays
            }
            RedisBrokerError::AdminPlan(RedisAdminPlanError::NonPositivePageSize) => {
                Self::NonPositivePageSize
            }
            error => Self::Other(redis_broker_error_message(error)),
        }
    }
}
