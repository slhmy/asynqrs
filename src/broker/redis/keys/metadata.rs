pub fn server_info_key(hostname: &str, pid: i32, server_id: &str) -> String {
    format!("asynq:servers:{{{hostname}:{pid}:{server_id}}}")
}
pub fn workers_key(hostname: &str, pid: i32, server_id: &str) -> String {
    format!("asynq:workers:{{{hostname}:{pid}:{server_id}}}")
}
pub fn scheduler_entries_key(scheduler_id: &str) -> String {
    format!("asynq:schedulers:{{{scheduler_id}}}")
}
pub fn scheduler_history_key(entry_id: &str) -> String {
    format!("asynq:scheduler_history:{entry_id}")
}
