use super::*;

#[test]
fn task_info_mutable_accessors_match_upstream_public_field_semantics() {
    let message = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    let mut info = TaskInfo::new(
        message,
        TaskState::Completed,
        false,
        None,
        b"handler-result".to_vec(),
    );

    info.payload_mut()[0] = b'P';
    info.payload_mut().extend_from_slice(b"-v2");
    info.headers_mut()
        .insert("trace-id".to_owned(), "abc".to_owned());
    info.headers_mut()
        .insert("tenant".to_owned(), "acme".to_owned());
    info.result_mut().extend_from_slice(b"-v2");
    info.result_mut()[0] = b'H';

    assert_eq!(info.payload(), b"Payload-v2");
    assert_eq!(
        info.headers().get("trace-id").map(String::as_str),
        Some("abc")
    );
    assert_eq!(
        info.headers().get("tenant").map(String::as_str),
        Some("acme")
    );
    assert_eq!(info.result(), b"Handler-result-v2");
}

#[test]
fn task_info_string_mutable_accessors_match_upstream_public_field_semantics() {
    let message = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    let mut info = TaskInfo::new(message, TaskState::Retry, false, None, Vec::new());

    info.id_mut().push_str("task");
    info.id_mut().push_str("-v2");
    info.queue_mut().clear();
    info.queue_mut().push_str("critical");
    info.task_type_mut().push(':');
    info.task_type_mut().push_str("v2");
    info.last_err_mut().push_str("boom");
    info.group_mut().push_str("tenant-a");
    info.group_mut().push_str("-blue");

    assert_eq!(info.id(), "task-v2");
    assert_eq!(info.queue(), "critical");
    assert_eq!(info.task_type(), "email:welcome:v2");
    assert_eq!(info.last_error(), "boom");
    assert_eq!(info.group(), "tenant-a-blue");
}

#[test]
fn task_info_scalar_mutable_accessors_match_upstream_public_field_semantics() {
    let message = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    let mut info = TaskInfo::new(message, TaskState::Pending, false, None, Vec::new());

    *info.state_mut() = TaskState::Retry;
    *info.state_mut() = TaskState::Archived;
    *info.max_retry_mut() = 7;
    *info.max_retry_mut() += 1;
    *info.retried_mut() = 2;
    *info.retried_mut() += 3;
    *info.is_orphaned_mut() = true;
    *info.is_orphaned_mut() = false;

    assert_eq!(info.state(), TaskState::Archived);
    assert_eq!(info.max_retry(), 8);
    assert_eq!(info.retried(), 5);
    assert!(!info.is_orphaned());
}
