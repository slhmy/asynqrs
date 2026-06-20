use asynqrs::TaskPayload;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, TaskPayload)]
#[task_type = 42]
struct NonStringTaskType {
    user_id: u64,
}

fn main() {}
