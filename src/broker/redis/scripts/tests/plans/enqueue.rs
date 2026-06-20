use super::*;

#[test]
fn redis_enqueue_plans_match_script_shapes() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", b"payload".to_vec());
    let cases = [
        EnqueuePlan::from_task_with_options(
            &task,
            EnqueueOptions::new().queue(crate::QueueName::new("critical").unwrap()),
            now,
            "pending-id",
        )
        .unwrap(),
        EnqueuePlan::from_task_with_options(
            &task,
            EnqueueOptions::new()
                .queue(crate::QueueName::new("critical").unwrap())
                .unique_for(Duration::from_secs(300)),
            now,
            "pending-unique-id",
        )
        .unwrap(),
        EnqueuePlan::from_task_with_options(
            &task,
            EnqueueOptions::new()
                .queue(crate::QueueName::new("critical").unwrap())
                .process_in(Duration::from_secs(60)),
            now,
            "scheduled-id",
        )
        .unwrap(),
        EnqueuePlan::from_task_with_options(
            &task,
            EnqueueOptions::new()
                .queue(crate::QueueName::new("critical").unwrap())
                .process_in(Duration::from_secs(60))
                .unique_for(Duration::from_secs(300)),
            now,
            "scheduled-unique-id",
        )
        .unwrap(),
        EnqueuePlan::from_task_with_options(
            &task,
            EnqueueOptions::new()
                .queue(crate::QueueName::new("critical").unwrap())
                .group(crate::GroupName::new("tenant-a").unwrap()),
            now,
            "group-id",
        )
        .unwrap(),
        EnqueuePlan::from_task_with_options(
            &task,
            EnqueueOptions::new()
                .queue(crate::QueueName::new("critical").unwrap())
                .group(crate::GroupName::new("tenant-a").unwrap())
                .unique_for(Duration::from_secs(300)),
            now,
            "group-unique-id",
        )
        .unwrap(),
    ];

    for plan in cases {
        let redis_plan = RedisEnqueuePlan::from_enqueue_plan(&plan, now).unwrap();
        let RedisEnqueueOperation::EvalScript(call) = &redis_plan.operations()[1] else {
            panic!("expected script call");
        };
        call.validate().unwrap();
    }
}
