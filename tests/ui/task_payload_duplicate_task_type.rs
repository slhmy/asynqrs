use asynqrs::TaskPayload;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, TaskPayload)]
#[task_type = "email:welcome"]
#[task_type = "email:duplicate"]
struct DuplicateTaskType {
    user_id: u64,
}

fn main() {}
