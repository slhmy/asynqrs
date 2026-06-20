use asynqrs::TaskPayload;

#[derive(Debug, PartialEq, Eq, TaskPayload)]
#[task_type = "email:welcome"]
struct WelcomeEmail {
    user_id: u64,
}

fn main() {}
