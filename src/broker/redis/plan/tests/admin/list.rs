use super::*;

#[test]
fn plans_admin_task_list_commands() {
    let list = RedisListTasksPlan::from_queue_state_and_pagination(
        "critical",
        TaskState::Retry,
        Pagination::new(1, 10).unwrap(),
    )
    .unwrap();
    let call = list.call();
    assert_eq!(list.state(), TaskState::Retry);
    assert_eq!(call.script(), RedisScript::ListTasks);
    assert_eq!(call.keys(), &["asynq:{critical}:retry".to_owned()]);
    assert_eq!(
        call.args(),
        &[
            RedisArg::I64(10),
            RedisArg::I64(19),
            RedisArg::String("asynq:{critical}:t:".to_owned()),
            RedisArg::String("0".to_owned()),
        ]
    );
    let list_all = RedisListTasksPlan::from_queue_state_and_pagination(
        "critical",
        TaskState::Retry,
        Pagination::new(3, 0).unwrap(),
    )
    .unwrap();
    assert_eq!(
        list_all.call().args(),
        &[
            RedisArg::I64(0),
            RedisArg::I64(-1),
            RedisArg::String("asynq:{critical}:t:".to_owned()),
            RedisArg::String("0".to_owned()),
        ]
    );
    let list_page_zero = RedisListTasksPlan::from_queue_state_and_pagination(
        "critical",
        TaskState::Retry,
        Pagination::from_asynq_options(0, 10).unwrap(),
    )
    .unwrap();
    assert_eq!(
        list_page_zero.call().args(),
        &[
            RedisArg::I64(-10),
            RedisArg::I64(-1),
            RedisArg::String("asynq:{critical}:t:".to_owned()),
            RedisArg::String("0".to_owned()),
        ]
    );

    let pending = RedisListTasksPlan::from_queue_state_and_pagination(
        "critical",
        TaskState::Pending,
        Pagination::new(1, 10).unwrap(),
    )
    .unwrap();
    let call = pending.call();
    assert_eq!(pending.state(), TaskState::Pending);
    assert_eq!(call.script(), RedisScript::ListTasks);
    assert_eq!(call.keys(), &["asynq:{critical}:pending".to_owned()]);
    assert_eq!(
        call.args(),
        &[
            RedisArg::I64(-20),
            RedisArg::I64(-11),
            RedisArg::String("asynq:{critical}:t:".to_owned()),
            RedisArg::String("1".to_owned()),
        ]
    );
    let pending_all = RedisListTasksPlan::from_queue_state_and_pagination(
        "critical",
        TaskState::Pending,
        Pagination::new(3, 0).unwrap(),
    )
    .unwrap();
    assert_eq!(
        pending_all.call().args(),
        &[
            RedisArg::I64(0),
            RedisArg::I64(-1),
            RedisArg::String("asynq:{critical}:t:".to_owned()),
            RedisArg::String("1".to_owned()),
        ]
    );
    let pending_page_zero = RedisListTasksPlan::from_queue_state_and_pagination(
        "critical",
        TaskState::Pending,
        Pagination::from_asynq_options(0, 10).unwrap(),
    )
    .unwrap();
    assert_eq!(
        pending_page_zero.call().args(),
        &[
            RedisArg::I64(0),
            RedisArg::I64(9),
            RedisArg::String("asynq:{critical}:t:".to_owned()),
            RedisArg::String("1".to_owned()),
        ]
    );

    let aggregating = RedisListAggregatingTasksPlan::from_queue_group_and_pagination(
        "critical",
        "tenant-a",
        Pagination::new(2, 5).unwrap(),
    )
    .unwrap();
    let call = aggregating.call();
    assert_eq!(call.script(), RedisScript::ListTasks);
    assert_eq!(call.keys(), &["asynq:{critical}:g:tenant-a".to_owned()]);
    assert_eq!(
        call.args(),
        &[
            RedisArg::I64(10),
            RedisArg::I64(14),
            RedisArg::String("asynq:{critical}:t:".to_owned()),
            RedisArg::String("0".to_owned()),
        ]
    );
    let aggregating_all = RedisListAggregatingTasksPlan::from_queue_group_and_pagination(
        "critical",
        "tenant-a",
        Pagination::new(3, 0).unwrap(),
    )
    .unwrap();
    assert_eq!(
        aggregating_all.call().args(),
        &[
            RedisArg::I64(0),
            RedisArg::I64(-1),
            RedisArg::String("asynq:{critical}:t:".to_owned()),
            RedisArg::String("0".to_owned()),
        ]
    );

    let aggregating_blank_group = RedisListAggregatingTasksPlan::from_queue_group_and_pagination(
        "critical",
        " ",
        Pagination::new(0, 10).unwrap(),
    )
    .unwrap();
    assert_eq!(
        aggregating_blank_group.call().keys(),
        &["asynq:{critical}:g: ".to_owned()]
    );
}
