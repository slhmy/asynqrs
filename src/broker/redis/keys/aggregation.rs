use super::queue::queue_key_prefix;

pub fn group_key_prefix(queue: &str) -> String {
    format!("{}g:", queue_key_prefix(queue))
}
pub fn group_key(queue: &str, group: &str) -> String {
    format!("{}{group}", group_key_prefix(queue))
}
pub fn aggregation_set_key(queue: &str, group: &str, set_id: &str) -> String {
    format!("{}:{set_id}", group_key(queue, group))
}
pub fn all_groups_key(queue: &str) -> String {
    format!("{}groups", queue_key_prefix(queue))
}
pub fn all_aggregation_sets_key(queue: &str) -> String {
    format!("{}aggregation_sets", queue_key_prefix(queue))
}
