use asynqrs::{GroupAggregatorFunc, Task};

fn main() {
    let mut aggregator = GroupAggregatorFunc(|group: &str, tasks: Vec<Task>| {
        let payload = format!(r#"{{"group":"{group}","count":{}}}"#, tasks.len());
        Task::new("batch:group", payload.into_bytes())
    });

    let task = aggregator.aggregate("tenant-a", vec![Task::new("email:welcome", b"{}".to_vec())]);
    println!("aggregated type={}", task.task_type());
}
