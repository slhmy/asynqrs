use super::*;

#[test]
fn task_info_time_mutable_accessors_match_upstream_public_field_semantics() {
    let first_time = UNIX_EPOCH + Duration::from_secs(1_700_000_100);
    let second_time = UNIX_EPOCH + Duration::from_secs(1_700_000_200);
    let mut message = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    let mut info = TaskInfo::new(message.clone(), TaskState::Retry, false, None, Vec::new());

    *info.last_failed_at_unix_seconds_mut() = 1_700_000_100;
    *info.last_failed_at_unix_seconds_mut() += 0;
    *info.timeout_seconds_mut() = 44;
    *info.timeout_seconds_mut() += 1;
    *info.deadline_unix_seconds_mut() = 1_700_000_199;
    *info.deadline_unix_seconds_mut() += 1;
    *info.retention_seconds_mut() = -10;
    *info.retention_seconds_mut() += 1;
    *info.completed_at_unix_seconds_mut() = -1;
    *info.completed_at_unix_seconds_mut() += 1;
    *info.next_process_at_mut() = Some(first_time);
    *info.next_process_at_mut() = Some(second_time);

    assert_eq!(info.last_failed_at(), Some(first_time));
    assert_eq!(info.timeout(), Duration::from_secs(45));
    assert_eq!(info.timeout_seconds(), 45);
    assert_eq!(info.deadline(), Some(second_time));
    assert_eq!(info.retention_seconds(), -9);
    assert_eq!(info.retention(), Duration::ZERO);
    assert_eq!(info.completed_at(), None);
    assert_eq!(info.next_process_at(), Some(second_time));

    message.last_failed_at = -5;
    message.timeout = -11;
    message.deadline = -7;
    message.retention = 12;
    message.completed_at = 1_700_000_100;
    let info = TaskInfo::new(message, TaskState::Completed, false, None, Vec::new());

    assert_eq!(
        info.last_failed_at(),
        Some(UNIX_EPOCH - Duration::from_secs(5))
    );
    assert_eq!(info.timeout_seconds(), -11);
    assert_eq!(info.deadline(), Some(UNIX_EPOCH - Duration::from_secs(7)));
    assert_eq!(info.retention_seconds(), 12);
    assert_eq!(info.completed_at(), Some(first_time));
}

#[test]
fn task_info_exposes_signed_task_message_duration_seconds() {
    let mut message = TaskMessage::from_task(&Task::new("email:welcome", Vec::new()));
    message.timeout = -5;
    message.retention = -9;
    let info = TaskInfo::new(message, TaskState::Pending, false, None, Vec::new());

    assert_eq!(info.timeout_seconds(), -5);
    assert_eq!(info.retention_seconds(), -9);
    assert_eq!(info.timeout(), Duration::ZERO);
    assert_eq!(info.retention(), Duration::ZERO);
}
