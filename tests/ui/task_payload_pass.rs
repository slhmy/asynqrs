use asynqrs::{TaskPayload, TypedTaskPayload};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, TaskPayload)]
#[task_type = "email:welcome"]
struct WelcomeEmail {
    user_id: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let task = WelcomeEmail { user_id: 42 }.into_task()?;

    assert_eq!(task.task_type(), WelcomeEmail::TASK_TYPE);
    assert_eq!(
        WelcomeEmail::decode_payload(task.payload())?,
        WelcomeEmail { user_id: 42 }
    );
    Ok(())
}
