use super::*;

#[test]
fn plans_pending_enqueue_script() {
    let plan_now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let exec_now = plan_now + Duration::from_secs(10);
    let task = Task::new("email:welcome", b"payload".to_vec());
    let enqueue_plan = plan_with(
        &task,
        EnqueueOptions::new().queue(crate::QueueName::new("critical").unwrap()),
        plan_now,
        "task-id",
    );

    let redis_plan = RedisEnqueuePlan::from_enqueue_plan(&enqueue_plan, exec_now).unwrap();

    assert_eq!(
        redis_plan.operations()[0],
        RedisEnqueueOperation::PublishQueue {
            key: keys::ALL_QUEUES.to_owned(),
            queue: "critical".to_owned()
        }
    );
    let call = only_script(&redis_plan);
    assert_eq!(call.script(), RedisScript::Enqueue);
    assert_eq!(
        call.keys(),
        &[
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:pending".to_owned(),
        ]
    );
    assert_encoded_message(&call.args()[0], enqueue_plan.message());
    assert_eq!(call.args()[1], RedisArg::String("task-id".to_owned()));
    assert_eq!(call.args()[2], RedisArg::I64(1_700_000_010_000_000_000));
}

#[test]
fn plans_pending_unique_enqueue_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", b"payload".to_vec());
    let enqueue_plan = plan_with(
        &task,
        EnqueueOptions::new()
            .queue(crate::QueueName::new("critical").unwrap())
            .unique_for(Duration::from_secs(300)),
        now,
        "task-id",
    );

    let redis_plan = RedisEnqueuePlan::from_enqueue_plan(&enqueue_plan, now).unwrap();

    let call = only_script(&redis_plan);
    assert_eq!(call.script(), RedisScript::EnqueueUnique);
    assert_eq!(
        call.keys(),
        &[
            "asynq:{critical}:unique:email:welcome:321c3cf486ed509164edec1e1981fec8".to_owned(),
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:pending".to_owned(),
        ]
    );
    assert_eq!(call.args()[0], RedisArg::String("task-id".to_owned()));
    assert_eq!(call.args()[1], RedisArg::I64(300));
    assert_encoded_message(&call.args()[2], enqueue_plan.message());
    assert_eq!(call.args()[3], RedisArg::I64(1_700_000_000_000_000_000));
}

#[test]
fn plans_scheduled_enqueue_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let process_at = now + Duration::from_secs(60);
    let task = Task::new("email:welcome", Vec::new());
    let enqueue_plan = plan_with(
        &task,
        EnqueueOptions::new()
            .queue(crate::QueueName::new("critical").unwrap())
            .process_at(process_at),
        now,
        "task-id",
    );

    let redis_plan = RedisEnqueuePlan::from_enqueue_plan(&enqueue_plan, now).unwrap();

    let call = only_script(&redis_plan);
    assert_eq!(call.script(), RedisScript::Schedule);
    assert_eq!(
        call.keys(),
        &[
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:scheduled".to_owned(),
        ]
    );
    assert_encoded_message(&call.args()[0], enqueue_plan.message());
    assert_eq!(call.args()[1], RedisArg::I64(1_700_000_060));
    assert_eq!(call.args()[2], RedisArg::String("task-id".to_owned()));
}

#[test]
fn plans_scheduled_unique_enqueue_script_with_extended_unique_ttl() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", b"payload".to_vec());
    let enqueue_plan = plan_with(
        &task,
        EnqueueOptions::new()
            .queue(crate::QueueName::new("critical").unwrap())
            .process_in(Duration::from_secs(60))
            .unique_for(Duration::from_secs(300)),
        now,
        "task-id",
    );

    let redis_plan = RedisEnqueuePlan::from_enqueue_plan(&enqueue_plan, now).unwrap();

    let call = only_script(&redis_plan);
    assert_eq!(call.script(), RedisScript::ScheduleUnique);
    assert_eq!(
        call.keys(),
        &[
            "asynq:{critical}:unique:email:welcome:321c3cf486ed509164edec1e1981fec8".to_owned(),
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:scheduled".to_owned(),
        ]
    );
    assert_eq!(call.args()[0], RedisArg::String("task-id".to_owned()));
    assert_eq!(call.args()[1], RedisArg::I64(360));
    assert_eq!(call.args()[2], RedisArg::I64(1_700_000_060));
    assert_encoded_message(&call.args()[3], enqueue_plan.message());
}

#[test]
fn plans_group_enqueue_script() {
    let plan_now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let exec_now = plan_now + Duration::from_secs(10);
    let task = Task::new("email:welcome", Vec::new());
    let enqueue_plan = plan_with(
        &task,
        EnqueueOptions::new()
            .queue(crate::QueueName::new("critical").unwrap())
            .group(crate::GroupName::new("tenant-a").unwrap()),
        plan_now,
        "task-id",
    );

    let redis_plan = RedisEnqueuePlan::from_enqueue_plan(&enqueue_plan, exec_now).unwrap();

    let call = only_script(&redis_plan);
    assert_eq!(call.script(), RedisScript::AddToGroup);
    assert_eq!(
        call.keys(),
        &[
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:g:tenant-a".to_owned(),
            "asynq:{critical}:groups".to_owned(),
        ]
    );
    assert_encoded_message(&call.args()[0], enqueue_plan.message());
    assert_eq!(call.args()[1], RedisArg::String("task-id".to_owned()));
    assert_eq!(call.args()[2], RedisArg::I64(1_700_000_010));
    assert_eq!(call.args()[3], RedisArg::String("tenant-a".to_owned()));
}

#[test]
fn plans_group_unique_enqueue_script() {
    let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let task = Task::new("email:welcome", b"payload".to_vec());
    let enqueue_plan = plan_with(
        &task,
        EnqueueOptions::new()
            .queue(crate::QueueName::new("critical").unwrap())
            .group(crate::GroupName::new("tenant-a").unwrap())
            .unique_for(Duration::from_secs(300)),
        now,
        "task-id",
    );

    let redis_plan = RedisEnqueuePlan::from_enqueue_plan(&enqueue_plan, now).unwrap();

    let call = only_script(&redis_plan);
    assert_eq!(call.script(), RedisScript::AddToGroupUnique);
    assert_eq!(
        call.keys(),
        &[
            "asynq:{critical}:t:task-id".to_owned(),
            "asynq:{critical}:g:tenant-a".to_owned(),
            "asynq:{critical}:groups".to_owned(),
            "asynq:{critical}:unique:email:welcome:321c3cf486ed509164edec1e1981fec8".to_owned(),
        ]
    );
    assert_encoded_message(&call.args()[0], enqueue_plan.message());
    assert_eq!(call.args()[1], RedisArg::String("task-id".to_owned()));
    assert_eq!(call.args()[2], RedisArg::I64(1_700_000_000));
    assert_eq!(call.args()[3], RedisArg::String("tenant-a".to_owned()));
    assert_eq!(call.args()[4], RedisArg::I64(300));
}
