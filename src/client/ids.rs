use std::time::SystemTime;

/// Generates task IDs for enqueue operations.
///
/// Reference: Asynq v0.26.0 `composeOptions` defaults task IDs with
/// `uuid.NewString`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L217-L264>.
pub trait TaskIdGenerator {
    fn generate_task_id(&mut self) -> String;
}

pub trait Clock {
    fn now(&self) -> SystemTime;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct UuidTaskIdGenerator;

impl TaskIdGenerator for UuidTaskIdGenerator {
    fn generate_task_id(&mut self) -> String {
        uuid::Uuid::new_v4().to_string()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}
