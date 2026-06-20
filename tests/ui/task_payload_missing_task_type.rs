use asynqrs::TaskPayload;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, TaskPayload)]
struct MissingTaskType {
    user_id: u64,
}

fn main() {}
