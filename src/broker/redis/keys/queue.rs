pub fn queue_key_prefix(queue: &str) -> String {
    format!("asynq:{{{queue}}}:")
}
pub fn task_key_prefix(queue: &str) -> String {
    format!("{}t:", queue_key_prefix(queue))
}
pub fn task_key(queue: &str, task_id: &str) -> String {
    format!("{}{task_id}", task_key_prefix(queue))
}
pub fn pending_key(queue: &str) -> String {
    format!("{}pending", queue_key_prefix(queue))
}
pub fn active_key(queue: &str) -> String {
    format!("{}active", queue_key_prefix(queue))
}
pub fn scheduled_key(queue: &str) -> String {
    format!("{}scheduled", queue_key_prefix(queue))
}
pub fn retry_key(queue: &str) -> String {
    format!("{}retry", queue_key_prefix(queue))
}
pub fn archived_key(queue: &str) -> String {
    format!("{}archived", queue_key_prefix(queue))
}
pub fn lease_key(queue: &str) -> String {
    format!("{}lease", queue_key_prefix(queue))
}
pub fn completed_key(queue: &str) -> String {
    format!("{}completed", queue_key_prefix(queue))
}
pub fn paused_key(queue: &str) -> String {
    format!("{}paused", queue_key_prefix(queue))
}
