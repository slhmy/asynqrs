use asynqrs::TaskPayload;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, TaskPayload)]
#[task_type = " "]
struct BlankTaskType {
    user_id: u64,
}

fn main() {}
