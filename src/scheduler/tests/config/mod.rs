use super::*;

mod builders;
mod constructors;
mod defaults;
mod time;

fn assert_generated_scheduler_id(scheduler_id: &str) {
    let mut parts = scheduler_id.rsplitn(3, ':');
    let uuid = parts.next().unwrap();
    let pid = parts.next().unwrap();
    let host = parts.next().unwrap();

    assert!(!host.is_empty());
    assert_eq!(pid, std::process::id().to_string());
    assert!(uuid::Uuid::parse_str(uuid).is_ok());
}

fn assert_scheduler_future<F>(_future: F)
where
    F: std::future::Future<Output = Result<RedisBackedScheduler, SchedulerConstructionError>>,
{
}
