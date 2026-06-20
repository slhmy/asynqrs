use super::*;

#[test]
fn small_inspector_models_expose_asynq_field_accessors() {
    let mut node = ClusterNode::new("node-a".to_owned(), "127.0.0.1:7000".to_owned());
    let enqueued_at = UNIX_EPOCH + Duration::from_secs(1_700_000_123);
    let next_enqueued_at = enqueued_at + Duration::from_secs(30);
    let mut event: SchedulerEnqueueEvent =
        SchedulerEnqueueEventInfo::new("task-id".to_owned(), enqueued_at);

    node.id_mut().push_str("-primary");
    node.id_mut().push_str("-replica");
    node.addr_mut().push_str("/0");
    node.addr_mut().push_str("/1");
    event.task_id_mut().push_str("-a");
    event.task_id_mut().push_str("-b");
    *event.enqueued_at_mut() = next_enqueued_at;
    *event.enqueued_at_mut() = enqueued_at;

    assert_eq!(node.id(), "node-a-primary-replica");
    assert_eq!(node.id(), "node-a-primary-replica");
    assert_eq!(node.addr(), "127.0.0.1:7000/0/1");
    assert_eq!(node.addr(), "127.0.0.1:7000/0/1");
    assert_eq!(event.task_id(), "task-id-a-b");
    assert_eq!(event.task_id(), "task-id-a-b");
    assert_eq!(event.enqueued_at(), enqueued_at);
    assert_eq!(event.enqueued_at(), enqueued_at);
}

#[test]
fn small_inspector_model_constructor_values_are_readable() {
    let node = ClusterNode::new("node-a".to_owned(), "127.0.0.1:7000".to_owned());
    let enqueued_at = UNIX_EPOCH + Duration::from_secs(1_700_000_123);
    let event: SchedulerEnqueueEvent =
        SchedulerEnqueueEventInfo::new("task-id".to_owned(), enqueued_at);

    assert_eq!(node.id(), "node-a");
    assert_eq!(node.id(), "node-a");
    assert_eq!(node.addr(), "127.0.0.1:7000");
    assert_eq!(node.addr(), "127.0.0.1:7000");
    assert_eq!(event.task_id(), "task-id");
    assert_eq!(event.task_id(), "task-id");
    assert_eq!(event.enqueued_at(), enqueued_at);
    assert_eq!(event.enqueued_at(), enqueued_at);
}
