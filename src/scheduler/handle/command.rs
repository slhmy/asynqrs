use std::time::Duration;

use tokio::sync::oneshot;

use crate::{EnqueueOptions, Task};

use super::super::SchedulerError;

#[derive(Debug)]
pub(in crate::scheduler) enum SchedulerCommand {
    RegisterEvery {
        entry_id: String,
        task: Task,
        spec: String,
        interval: Duration,
        options: EnqueueOptions,
        result: oneshot::Sender<Result<(), SchedulerError>>,
    },
    Register {
        entry_id: String,
        task: Task,
        spec: String,
        options: EnqueueOptions,
        result: oneshot::Sender<Result<(), SchedulerError>>,
    },
    Unregister {
        entry_id: String,
        result: oneshot::Sender<Result<(), SchedulerError>>,
    },
    Ping {
        result: oneshot::Sender<Result<(), SchedulerError>>,
    },
}
