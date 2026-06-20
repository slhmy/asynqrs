use super::*;

#[test]
fn update_task_payload_precondition_error_text_matches_upstream() {
    assert_eq!(
        AdminError::TaskNotScheduled.to_string(),
        "cannot update task that is not in scheduled state."
    );
}

#[test]
fn task_lifecycle_precondition_error_texts_match_upstream() {
    assert_eq!(
        AdminError::TaskAlreadyRunning.to_string(),
        "task is already running"
    );
    assert_eq!(
        AdminError::TaskAlreadyPending.to_string(),
        "task is already in pending state"
    );
    assert_eq!(
        AdminError::CannotDeleteActiveTask.to_string(),
        "cannot delete task in active state. use CancelProcessing instead."
    );
    assert_eq!(
        AdminError::CannotArchiveActiveTask.to_string(),
        "cannot archive task in active state. use CancelProcessing instead."
    );
}

#[test]
fn inspector_error_sentinels_match_upstream_names() {
    assert_eq!(AdminError::QueueNotFound.to_string(), "queue not found");
    assert_eq!(
        AdminError::QueueNotFoundForQueue {
            queue: "critical".to_owned()
        }
        .to_string(),
        "queue not found: queue=\"critical\""
    );
    assert_eq!(AdminError::QueueNotEmpty.to_string(), "queue is not empty");
    assert_eq!(
        AdminError::QueueNotEmptyForQueue {
            queue: "critical".to_owned()
        }
        .to_string(),
        "queue is not empty: queue=\"critical\""
    );
    assert_eq!(AdminError::TaskNotFound.to_string(), "task not found");
    assert_eq!(
        AdminError::AsynqQueueNotFound.to_string(),
        "asynq: queue not found"
    );
    assert_eq!(
        AdminError::AsynqTaskNotFound.to_string(),
        "asynq: task not found"
    );
    assert_eq!(
        AdminError::InvalidQueueName.to_string(),
        "queue name must contain one or more characters"
    );
    assert_eq!(
        AdminError::AsynqInvalidQueueName.to_string(),
        "asynq: queue name must contain one or more characters"
    );
    assert_eq!(
        AdminError::AsynqArchiveQueueValidation.to_string(),
        "asynq: err"
    );
    assert_eq!(
        AdminError::QueueHasActiveTasksForRemoval.to_string(),
        "cannot remove queue with active tasks"
    );
}

#[test]
fn admin_error_sentinel_predicates_match_upstream_errors_is_checks() {
    let queue_missing = AdminError::QueueNotFound;
    let queue_not_empty = AdminError::QueueNotEmpty;
    let task_missing = AdminError::TaskNotFound;
    let other = AdminError::Other("redis down".to_owned());

    assert!(AdminError::QueueNotFound.is_queue_not_found());
    assert!(!AdminError::QueueNotFound.is_queue_not_empty());
    assert!(!AdminError::QueueNotFound.is_task_not_found());
    assert!(!AdminError::QueueNotFound.is_invalid_queue_name());
    assert!(!AdminError::QueueNotFound.is_queue_has_active_tasks());
    assert!(AdminError::QueueNotEmpty.is_queue_not_empty());
    assert!(!AdminError::QueueNotEmpty.is_queue_not_found());
    assert!(!AdminError::QueueNotEmpty.is_task_not_found());
    assert!(AdminError::TaskNotFound.is_task_not_found());
    assert!(!AdminError::TaskNotFound.is_queue_not_found());
    assert!(!AdminError::TaskNotFound.is_queue_not_empty());
    assert!(AdminError::InvalidQueueName.is_invalid_queue_name());
    assert!(AdminError::AsynqInvalidQueueName.is_invalid_queue_name());
    assert!(AdminError::QueueHasActiveTasks.is_queue_has_active_tasks());
    assert!(AdminError::QueueHasActiveTasksForRemoval.is_queue_has_active_tasks());

    assert!(queue_missing.is_queue_not_found());
    assert!(
        AdminError::QueueNotFoundForQueue {
            queue: "critical".to_owned()
        }
        .is_queue_not_found()
    );
    assert!(AdminError::AsynqQueueNotFound.is_queue_not_found());
    assert!(queue_not_empty.is_queue_not_empty());
    assert!(
        AdminError::QueueNotEmptyForQueue {
            queue: "critical".to_owned()
        }
        .is_queue_not_empty()
    );
    assert!(task_missing.is_task_not_found());
    assert!(AdminError::AsynqTaskNotFound.is_task_not_found());
    assert!(!other.is_queue_not_found());
    assert!(!other.is_queue_not_empty());
    assert!(!other.is_task_not_found());
    assert!(!other.is_invalid_queue_name());
    assert!(!other.is_queue_has_active_tasks());
}
