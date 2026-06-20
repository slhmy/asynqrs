use super::*;

#[test]
fn task_info_exposes_asynq_task_info_fields() {
    let last_failed_at = UNIX_EPOCH + Duration::from_secs(1_700_000_001);
    let deadline = UNIX_EPOCH + Duration::from_secs(1_700_000_120);
    let completed_at = UNIX_EPOCH + Duration::from_secs(1_700_000_240);
    let next_process_at = UNIX_EPOCH + Duration::from_secs(1_700_000_060);
    let mut message = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    message.id = "task-id".to_owned();
    message.queue = "critical".to_owned();
    message.headers.insert("trace-id".into(), "abc".into());
    message.retry = 7;
    message.retried = 2;
    message.error_msg = "boom".to_owned();
    message.last_failed_at = 1_700_000_001;
    message.timeout = 45;
    message.deadline = 1_700_000_120;
    message.group_key = "tenant-a".to_owned();
    message.retention = 300;
    message.completed_at = 1_700_000_240;

    let mut info = TaskInfo::new(
        message,
        TaskState::Retry,
        false,
        Some(next_process_at),
        b"handler-result".to_vec(),
    );
    info.mark_orphaned();

    assert_eq!(info.id(), "task-id");
    assert_eq!(info.id(), "task-id");
    assert_eq!(info.queue(), "critical");
    assert_eq!(info.queue(), "critical");
    assert_eq!(info.task_type(), "email:welcome");
    assert_eq!(info.task_type(), "email:welcome");
    assert_eq!(info.type_name(), "email:welcome");
    assert_eq!(info.payload(), b"payload");
    assert_eq!(info.payload(), b"payload");
    assert_eq!(info.payload_bytes(), bytes::Bytes::from_static(b"payload"));
    assert_eq!(
        info.headers().get("trace-id").map(String::as_str),
        Some("abc")
    );
    assert_eq!(
        info.headers().get("trace-id").map(String::as_str),
        Some("abc")
    );
    assert_eq!(info.max_retry(), 7);
    assert_eq!(info.max_retry(), 7);
    assert_eq!(info.retried(), 2);
    assert_eq!(info.retried(), 2);
    assert_eq!(info.last_err(), "boom");
    assert_eq!(info.last_error(), "boom");
    assert_eq!(info.last_error(), "boom");
    assert_eq!(info.last_failed_at(), Some(last_failed_at));
    assert_eq!(info.last_failed_at(), Some(last_failed_at));
    assert_eq!(info.timeout(), Duration::from_secs(45));
    assert_eq!(info.timeout(), Duration::from_secs(45));
    assert_eq!(info.timeout_seconds(), 45);
    assert_eq!(info.deadline(), Some(deadline));
    assert_eq!(info.deadline(), Some(deadline));
    assert_eq!(info.group(), "tenant-a");
    assert_eq!(info.group(), "tenant-a");
    assert!(info.is_orphaned());
    assert!(info.is_orphaned());
    assert_eq!(info.retention(), Duration::from_secs(300));
    assert_eq!(info.retention(), Duration::from_secs(300));
    assert_eq!(info.retention_seconds(), 300);
    assert_eq!(info.completed_at(), Some(completed_at));
    assert_eq!(info.completed_at(), Some(completed_at));
    assert_eq!(info.state(), TaskState::Retry);
    assert_eq!(info.state(), TaskState::Retry);
    assert_eq!(info.next_process_at(), Some(next_process_at));
    assert_eq!(info.next_process_at(), Some(next_process_at));
    assert_eq!(info.result(), b"handler-result");
    assert_eq!(info.result(), b"handler-result");
    assert_eq!(
        info.result_bytes(),
        bytes::Bytes::from_static(b"handler-result")
    );
}

#[test]
fn task_info_payload_and_result_can_be_taken_as_owned_bytes() {
    let payload_info = TaskInfo::new(
        TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec())),
        TaskState::Pending,
        false,
        None,
        b"handler-result".to_vec(),
    );
    let result_info = payload_info.clone();

    assert_eq!(payload_info.into_payload(), b"payload");
    assert_eq!(result_info.into_result(), b"handler-result");
}
