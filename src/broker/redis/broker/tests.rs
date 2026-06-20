use super::*;
use prost::Message;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::aggregation::AggregationBroker;
use crate::broker::redis::executor::RedisSlotRange;
use crate::broker::redis::{RedisArg, RedisScript};
use crate::client::CloseBroker;
use crate::server::{ArchiveBroker, LeaseBroker, RequeueBroker, RetryBroker};
use crate::task::TaskMessage;
use crate::{
    AdminError, ArchiveError, BrokerError, CancelError, ClusterNode, CompleteError, DequeueError,
    EnqueueOptions, EnqueuePlan, Pagination, RequeueError, ResultError, RetryError, Task,
    TaskState, page, page_size, pb,
};
use operations::AggregationCheckRequest;

mod fixtures;

use fixtures::*;

mod aggregating_task_admin;
mod aggregation;
mod core;
mod lease;
mod lifecycle;
mod maintenance;
mod metadata_scheduler;
mod metadata_server;
mod queue_admin;
mod queue_stats;
mod result_cancel;
mod task_admin;
mod task_info;
mod task_listing;

fn plan_with(task: &Task, options: EnqueueOptions, now: SystemTime, task_id: &str) -> EnqueuePlan {
    EnqueuePlan::from_task_with_options(task, options, now, task_id).unwrap()
}
