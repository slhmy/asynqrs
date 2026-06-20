use super::*;

#[test]
fn inspector_from_redis_runtime_client_matches_shared_constructor_shape() {
    fn assert_future<F>(_future: F)
    where
        F: std::future::Future<Output = Result<Inspector, MakeRedisClientError>>,
    {
    }

    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();

    assert_future(Inspector::from_redis_runtime_client(
        RedisRuntimeClient::direct(redis_client),
    ));
}

#[test]
fn inspector_from_direct_redis_client_keeps_direct_client_convenience() {
    fn assert_future<F>(_future: F)
    where
        F: std::future::Future<Output = Result<Inspector, MakeRedisClientError>>,
    {
    }

    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();

    assert_future(Inspector::from_direct_redis_client(redis_client));
}

#[test]
fn inspector_from_redis_runtime_client_accepts_shared_runtime_boundary() {
    fn assert_future<F>(_future: F)
    where
        F: std::future::Future<Output = Result<Inspector, MakeRedisClientError>>,
    {
    }

    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();

    assert_future(Inspector::from_redis_runtime_client(
        RedisRuntimeClient::direct(redis_client),
    ));
}

#[test]
fn inspector_from_redis_client_matches_constructor_shape() {
    fn assert_future<F>(_future: F)
    where
        F: std::future::Future<Output = Result<Inspector, MakeRedisClientError>>,
    {
    }

    let redis_client = redis::Client::open("redis://localhost:6379").unwrap();

    assert_future(Inspector::from_redis_client(redis_client));
}

#[test]
fn inspector_close_refuses_shared_connections_like_upstream() {
    let mut inspector = shared_inspector_with_broker(RecordingCloseBroker::default());

    assert!(inspector.shared_connection);
    assert_eq!(inspector.close(), Err(InspectorError::SharedConnection));
    assert_eq!(inspector.close(), Err(InspectorError::SharedConnection));
    assert!(!inspector.broker.closed);
    assert_eq!(
        InspectorError::SharedConnection.to_string(),
        "redis connection is shared so the Inspector can't be closed through asynq"
    );
}

#[test]
fn inspector_close_delegates_to_owned_broker_like_upstream() {
    let mut inspector = inspector_with_broker(RecordingCloseBroker::default());

    assert!(!inspector.shared_connection);
    assert_eq!(inspector.close(), Ok(()));
    assert!(inspector.broker.closed);
}

#[test]
fn inspector_close_maps_owned_broker_errors() {
    let mut inspector = inspector_with_broker(RecordingCloseBroker {
        closed: false,
        error: Some(BrokerError::Other("redis down".to_owned())),
    });

    let error = inspector.close().unwrap_err();

    assert_eq!(
        error,
        InspectorError::Close(BrokerError::Other("redis down".to_owned()))
    );
    assert_eq!(error.to_string(), "redis down");
    assert!(inspector.broker.closed);
}

#[tokio::test]
async fn inspector_cancel_processing_publishes_task_id_like_upstream() {
    let mut inspector = inspector_with_broker(RecordingCancelBroker {
        subscribers: 3,
        ..RecordingCancelBroker::default()
    });

    inspector.cancel_processing("task-id").await.unwrap();
    inspector.cancel_processing("second-id").await.unwrap();

    assert_eq!(
        inspector.broker.published.as_slice(),
        ["task-id".to_owned(), "second-id".to_owned()]
    );
}

#[tokio::test]
async fn inspector_cancel_processing_maps_broker_errors() {
    let mut inspector = inspector_with_broker(RecordingCancelBroker {
        error: Some(CancelError::Other("redis down".to_owned())),
        ..RecordingCancelBroker::default()
    });

    let error = inspector.cancel_processing("task-id").await.unwrap_err();

    assert_eq!(error, CancelError::Other("redis down".to_owned()));
    assert_eq!(inspector.broker.published.as_slice(), ["task-id"]);
}

#[tokio::test]
async fn inspector_stats_methods_delegate_to_broker_like_upstream() {
    let mut inspector = inspector_with_broker(RecordingStatsBroker {
        queues: vec!["critical".to_owned(), "bulk".to_owned()],
        groups: vec![GroupStats::new("tenant-a".to_owned(), 3)],
        history: vec![DailyStats::new("critical".to_owned(), 7, 2, UNIX_EPOCH)],
        ..RecordingStatsBroker::default()
    });

    assert_eq!(
        inspector.queues().await.unwrap(),
        ["critical".to_owned(), "bulk".to_owned()]
    );
    assert_eq!(
        inspector.groups("critical").await.unwrap(),
        [GroupStats::new("tenant-a".to_owned(), 3)]
    );
    assert_eq!(
        inspector.get_queue_info("critical").await.unwrap(),
        RecordingStatsBroker::default().queue_stats
    );
    assert_eq!(
        inspector.history("critical", 7).await.unwrap(),
        [DailyStats::new("critical".to_owned(), 7, 2, UNIX_EPOCH)]
    );

    assert_eq!(
        inspector.broker.calls.as_slice(),
        [
            StatsCall::Queues,
            StatsCall::Groups {
                queue: "critical".to_owned(),
            },
            StatsCall::GetQueueInfo {
                queue: "critical".to_owned(),
            },
            StatsCall::History {
                queue: "critical".to_owned(),
                days: 7,
            },
        ]
    );
}

#[tokio::test]
async fn inspector_stats_methods_propagate_admin_errors() {
    let mut inspector = inspector_with_broker(RecordingStatsBroker {
        error: Some(AdminError::QueueNotFound),
        ..RecordingStatsBroker::default()
    });

    let error = inspector.history("critical", 7).await.unwrap_err();

    assert_eq!(error, AdminError::QueueNotFound);
    assert_eq!(
        inspector.broker.calls.as_slice(),
        [StatsCall::History {
            queue: "critical".to_owned(),
            days: 7,
        }]
    );
}

#[tokio::test]
async fn inspector_stats_methods_validate_queue_before_broker_like_upstream() {
    let mut inspector = inspector_with_broker(RecordingStatsBroker::default());
    let expected = AdminError::InvalidQueueName;

    assert_eq!(inspector.get_queue_info("").await.unwrap_err(), expected);
    assert_eq!(inspector.history(" ", 7).await.unwrap_err(), expected);
    assert!(inspector.broker.calls.is_empty());
}

#[tokio::test]
async fn inspector_history_delegates_negative_days_to_broker_like_upstream() {
    let mut inspector = inspector_with_broker(RecordingStatsBroker::default());

    inspector.history("critical", -1).await.unwrap();

    assert_eq!(
        inspector.broker.calls.as_slice(),
        [StatsCall::History {
            queue: "critical".to_owned(),
            days: -1,
        }]
    );
}

#[tokio::test]
async fn inspector_groups_allows_blank_queue_delegation_like_upstream() {
    let mut inspector = inspector_with_broker(RecordingStatsBroker::default());

    inspector.groups("").await.unwrap();

    assert_eq!(
        inspector.broker.calls.as_slice(),
        [StatsCall::Groups {
            queue: String::new(),
        }]
    );
}

#[tokio::test]
async fn inspector_task_read_methods_delegate_to_broker_like_upstream() {
    let task = sample_task_info("task-id", TaskState::Pending);
    let tasks = vec![sample_task_info("listed-id", TaskState::Scheduled)];
    let mut inspector = inspector_with_broker(RecordingTaskReadBroker {
        task: task.clone(),
        tasks: tasks.clone(),
        ..RecordingTaskReadBroker::default()
    });

    assert_eq!(
        inspector
            .get_task_info("critical", "task-id")
            .await
            .unwrap(),
        task
    );
    assert_eq!(
        inspector
            .list_pending_tasks_with_options("critical", [page_size(10), page(2)])
            .await
            .unwrap(),
        tasks
    );
    assert_eq!(
        inspector
            .list_active_tasks("critical", Pagination::new(0, 5).unwrap())
            .await
            .unwrap(),
        tasks
    );
    assert_eq!(
        inspector
            .list_aggregating_tasks_with_options("critical", "tenant-a", [page_size(3)])
            .await
            .unwrap(),
        tasks
    );
    assert_eq!(
        inspector
            .list_scheduled_tasks_with_options("critical", [page(3)])
            .await
            .unwrap(),
        tasks
    );
    assert_eq!(
        inspector
            .list_retry_tasks_with_options("critical", [page_size(4)])
            .await
            .unwrap(),
        tasks
    );
    assert_eq!(
        inspector
            .list_archived_tasks_with_options("critical", [page_size(2), page(1)])
            .await
            .unwrap(),
        tasks
    );
    assert_eq!(
        inspector
            .list_completed_tasks("critical", Pagination::new(4, 6).unwrap())
            .await
            .unwrap(),
        tasks
    );

    assert_eq!(
        inspector.broker.calls.as_slice(),
        [
            TaskReadCall::Get {
                queue: "critical".to_owned(),
                task_id: "task-id".to_owned(),
            },
            TaskReadCall::Pending {
                queue: "critical".to_owned(),
                pagination: Pagination::from_list_options([page_size(10), page(2)]).unwrap(),
            },
            TaskReadCall::Active {
                queue: "critical".to_owned(),
                pagination: Pagination::new(0, 5).unwrap(),
            },
            TaskReadCall::Aggregating {
                queue: "critical".to_owned(),
                group: "tenant-a".to_owned(),
                pagination: Pagination::from_list_options([page_size(3)]).unwrap(),
            },
            TaskReadCall::Scheduled {
                queue: "critical".to_owned(),
                pagination: Pagination::from_list_options([page(3)]).unwrap(),
            },
            TaskReadCall::Retry {
                queue: "critical".to_owned(),
                pagination: Pagination::from_list_options([page_size(4)]).unwrap(),
            },
            TaskReadCall::Archived {
                queue: "critical".to_owned(),
                pagination: Pagination::from_list_options([page_size(2), page(1)]).unwrap(),
            },
            TaskReadCall::Completed {
                queue: "critical".to_owned(),
                pagination: Pagination::new(4, 6).unwrap(),
            },
        ]
    );
}

#[tokio::test]
async fn inspector_task_read_methods_wrap_admin_errors_like_upstream() {
    let mut inspector = inspector_with_broker(RecordingTaskReadBroker {
        error: Some(AdminError::QueueNotFound),
        ..RecordingTaskReadBroker::default()
    });

    let error = inspector
        .list_scheduled_tasks_with_options("critical", [page_size(10)])
        .await
        .unwrap_err();

    assert_eq!(error, AdminError::AsynqQueueNotFound);
    assert!(error.is_queue_not_found());
    assert_eq!(
        inspector.broker.calls.as_slice(),
        [TaskReadCall::Scheduled {
            queue: "critical".to_owned(),
            pagination: Pagination::from_list_options([page_size(10)]).unwrap(),
        }]
    );

    let mut inspector = inspector_with_broker(RecordingTaskReadBroker {
        error: Some(AdminError::TaskNotFound),
        ..RecordingTaskReadBroker::default()
    });

    let error = inspector
        .get_task_info("critical", "missing")
        .await
        .unwrap_err();

    assert_eq!(error, AdminError::AsynqTaskNotFound);
    assert!(error.is_task_not_found());
    assert_eq!(
        inspector.broker.calls.as_slice(),
        [TaskReadCall::Get {
            queue: "critical".to_owned(),
            task_id: "missing".to_owned(),
        }]
    );

    let mut inspector = inspector_with_broker(RecordingTaskReadBroker {
        error: Some(AdminError::Other("redis down".to_owned())),
        ..RecordingTaskReadBroker::default()
    });

    let error = inspector
        .list_retry_tasks_with_options("critical", [page_size(10)])
        .await
        .unwrap_err();

    assert_eq!(error, AdminError::Other("asynq: redis down".to_owned()));
}

#[tokio::test]
async fn inspector_task_read_methods_validate_queue_before_broker_like_upstream() {
    let mut inspector = inspector_with_broker(RecordingTaskReadBroker::default());
    let expected = AdminError::AsynqInvalidQueueName;

    assert_eq!(
        inspector
            .list_pending_tasks_with_options("", [page_size(10)])
            .await
            .unwrap_err(),
        expected
    );
    assert_eq!(
        inspector
            .list_aggregating_tasks_with_options(" ", "tenant-a", [page_size(10)])
            .await
            .unwrap_err(),
        expected
    );
    assert!(inspector.broker.calls.is_empty());
}

#[tokio::test]
async fn inspector_get_task_info_allows_blank_queue_delegation_like_upstream() {
    let mut inspector = inspector_with_broker(RecordingTaskReadBroker::default());

    inspector.get_task_info("", "task-id").await.unwrap();

    assert_eq!(
        inspector.broker.calls.as_slice(),
        [TaskReadCall::Get {
            queue: String::new(),
            task_id: "task-id".to_owned(),
        }]
    );
}

#[tokio::test]
async fn inspector_bulk_methods_delegate_to_broker_like_upstream() {
    let mut inspector = inspector_with_broker(RecordingBulkBroker {
        affected: 7,
        ..RecordingBulkBroker::default()
    });

    assert_eq!(
        inspector.run_all_scheduled_tasks("critical").await.unwrap(),
        7
    );
    assert_eq!(inspector.run_all_retry_tasks("critical").await.unwrap(), 7);
    assert_eq!(inspector.run_all_archived_tasks("bulk").await.unwrap(), 7);
    assert_eq!(
        inspector
            .run_all_aggregating_tasks("critical", "tenant-a")
            .await
            .unwrap(),
        7
    );
    assert_eq!(
        inspector
            .archive_all_pending_tasks("critical")
            .await
            .unwrap(),
        7
    );
    assert_eq!(
        inspector
            .archive_all_scheduled_tasks("critical")
            .await
            .unwrap(),
        7
    );
    assert_eq!(inspector.archive_all_retry_tasks("bulk").await.unwrap(), 7);
    assert_eq!(
        inspector
            .archive_all_aggregating_tasks("critical", "tenant-b")
            .await
            .unwrap(),
        7
    );
    assert_eq!(
        inspector
            .delete_all_pending_tasks("critical")
            .await
            .unwrap(),
        7
    );
    assert_eq!(
        inspector
            .delete_all_scheduled_tasks("critical")
            .await
            .unwrap(),
        7
    );
    assert_eq!(inspector.delete_all_retry_tasks("bulk").await.unwrap(), 7);
    assert_eq!(
        inspector.delete_all_archived_tasks("bulk").await.unwrap(),
        7
    );
    assert_eq!(
        inspector
            .delete_all_completed_tasks("critical")
            .await
            .unwrap(),
        7
    );
    assert_eq!(
        inspector
            .delete_all_aggregating_tasks("critical", "tenant-c")
            .await
            .unwrap(),
        7
    );

    assert_eq!(
        inspector.broker.calls.as_slice(),
        [
            BulkCall::RunScheduled("critical".to_owned()),
            BulkCall::RunRetry("critical".to_owned()),
            BulkCall::RunArchived("bulk".to_owned()),
            BulkCall::RunAggregating {
                queue: "critical".to_owned(),
                group: "tenant-a".to_owned(),
            },
            BulkCall::ArchivePending("critical".to_owned()),
            BulkCall::ArchiveScheduled("critical".to_owned()),
            BulkCall::ArchiveRetry("bulk".to_owned()),
            BulkCall::ArchiveAggregating {
                queue: "critical".to_owned(),
                group: "tenant-b".to_owned(),
            },
            BulkCall::DeletePending("critical".to_owned()),
            BulkCall::DeleteScheduled("critical".to_owned()),
            BulkCall::DeleteRetry("bulk".to_owned()),
            BulkCall::DeleteArchived("bulk".to_owned()),
            BulkCall::DeleteCompleted("critical".to_owned()),
            BulkCall::DeleteAggregating {
                queue: "critical".to_owned(),
                group: "tenant-c".to_owned(),
            },
        ]
    );
}

#[tokio::test]
async fn inspector_bulk_methods_propagate_admin_errors() {
    let mut inspector = inspector_with_broker(RecordingBulkBroker {
        error: Some(AdminError::QueueNotFound),
        ..RecordingBulkBroker::default()
    });

    let error = inspector
        .delete_all_pending_tasks("critical")
        .await
        .unwrap_err();

    assert_eq!(error, AdminError::QueueNotFound);
    assert_eq!(
        inspector.broker.calls.as_slice(),
        [BulkCall::DeletePending("critical".to_owned())]
    );
}

#[tokio::test]
async fn inspector_bulk_methods_validate_queue_before_broker_like_upstream() {
    let mut inspector = inspector_with_broker(RecordingBulkBroker::default());
    let expected = AdminError::InvalidQueueName;

    assert_eq!(
        inspector.run_all_scheduled_tasks(" ").await.unwrap_err(),
        expected
    );
    assert_eq!(
        inspector
            .delete_all_aggregating_tasks("", "tenant-a")
            .await
            .unwrap_err(),
        expected
    );
    assert!(inspector.broker.calls.is_empty());
}

#[tokio::test]
async fn inspector_metadata_methods_delegate_to_broker_like_upstream() {
    let servers = vec![sample_server_info("server-a")];
    let cluster_nodes = vec![ClusterNode::new(
        "node-a".to_owned(),
        "127.0.0.1:6379".to_owned(),
    )];
    let scheduler_entries = vec![sample_scheduler_entry("entry-a")];
    let scheduler_events = vec![SchedulerEnqueueEventInfo::new(
        "task-id".to_owned(),
        UNIX_EPOCH,
    )];
    let mut inspector = inspector_with_broker(RecordingMetadataBroker {
        servers: servers.clone(),
        key_slot: 42,
        cluster_nodes: cluster_nodes.clone(),
        scheduler_entries: scheduler_entries.clone(),
        scheduler_events: scheduler_events.clone(),
        ..RecordingMetadataBroker::default()
    });

    assert_eq!(inspector.servers().await.unwrap(), servers);
    assert_eq!(inspector.cluster_key_slot("critical").await.unwrap(), 42);
    assert_eq!(
        inspector.cluster_nodes("critical").await.unwrap(),
        cluster_nodes
    );
    assert_eq!(
        inspector.scheduler_entries().await.unwrap(),
        scheduler_entries
    );
    assert_eq!(
        inspector
            .list_scheduler_enqueue_events_with_options("entry-a", [page_size(10), page(2)])
            .await
            .unwrap(),
        scheduler_events
    );

    assert_eq!(
        inspector.broker.calls.as_slice(),
        [
            MetadataCall::Servers,
            MetadataCall::ClusterKeySlot("critical".to_owned()),
            MetadataCall::ClusterNodes("critical".to_owned()),
            MetadataCall::SchedulerEntries,
            MetadataCall::SchedulerEvents {
                entry_id: "entry-a".to_owned(),
                pagination: Pagination::from_list_options([page_size(10), page(2)]).unwrap(),
            },
        ]
    );
}

#[tokio::test]
async fn inspector_metadata_methods_propagate_admin_errors() {
    let mut inspector = inspector_with_broker(RecordingMetadataBroker {
        error: Some(AdminError::QueueNotFound),
        ..RecordingMetadataBroker::default()
    });

    let error = inspector.cluster_nodes("critical").await.unwrap_err();

    assert_eq!(error, AdminError::QueueNotFound);
    assert_eq!(
        inspector.broker.calls.as_slice(),
        [MetadataCall::ClusterNodes("critical".to_owned())]
    );
}

#[tokio::test]
async fn inspector_queue_lifecycle_methods_delegate_to_broker_like_upstream() {
    let mut inspector = inspector_with_broker(RecordingQueueLifecycleBroker::default());

    inspector.pause_queue("critical").await.unwrap();
    inspector.unpause_queue("critical").await.unwrap();
    inspector.delete_queue("critical", false).await.unwrap();
    inspector.delete_queue("bulk", true).await.unwrap();

    assert_eq!(
        inspector.broker.calls.as_slice(),
        [
            QueueLifecycleCall::Pause("critical".to_owned()),
            QueueLifecycleCall::Unpause("critical".to_owned()),
            QueueLifecycleCall::Delete {
                queue: "critical".to_owned(),
                force: false,
            },
            QueueLifecycleCall::Delete {
                queue: "bulk".to_owned(),
                force: true,
            },
        ]
    );
}

#[tokio::test]
async fn inspector_queue_lifecycle_methods_propagate_admin_errors() {
    let mut inspector = inspector_with_broker(RecordingQueueLifecycleBroker {
        calls: Vec::new(),
        error: Some(AdminError::QueueNotFound),
    });

    let error = inspector.pause_queue("critical").await.unwrap_err();

    assert_eq!(error, AdminError::QueueNotFound);
    assert_eq!(
        inspector.broker.calls.as_slice(),
        [QueueLifecycleCall::Pause("critical".to_owned())]
    );
}

#[tokio::test]
async fn inspector_delete_queue_wraps_queue_errors_like_upstream() {
    let mut inspector = inspector_with_broker(RecordingQueueLifecycleBroker {
        calls: Vec::new(),
        error: Some(AdminError::QueueNotFound),
    });

    let error = inspector.delete_queue("critical", false).await.unwrap_err();

    assert_eq!(
        error,
        AdminError::QueueNotFoundForQueue {
            queue: "critical".to_owned()
        }
    );
    assert!(error.is_queue_not_found());
    assert_eq!(
        inspector.broker.calls.as_slice(),
        [QueueLifecycleCall::Delete {
            queue: "critical".to_owned(),
            force: false,
        }]
    );

    let mut inspector = inspector_with_broker(RecordingQueueLifecycleBroker {
        calls: Vec::new(),
        error: Some(AdminError::QueueNotEmpty),
    });

    let error = inspector.delete_queue("bulk", true).await.unwrap_err();

    assert_eq!(
        error,
        AdminError::QueueNotEmptyForQueue {
            queue: "bulk".to_owned()
        }
    );
    assert!(error.is_queue_not_empty());

    let mut inspector = inspector_with_broker(RecordingQueueLifecycleBroker {
        calls: Vec::new(),
        error: Some(AdminError::QueueHasActiveTasks),
    });

    let error = inspector.delete_queue("critical", true).await.unwrap_err();

    assert_eq!(error, AdminError::QueueHasActiveTasksForRemoval);
    assert!(error.is_queue_has_active_tasks());
}

#[tokio::test]
async fn inspector_queue_lifecycle_methods_validate_queue_before_broker_like_upstream() {
    let mut inspector = inspector_with_broker(RecordingQueueLifecycleBroker::default());
    let expected = AdminError::InvalidQueueName;

    assert_eq!(inspector.pause_queue(" ").await.unwrap_err(), expected);
    assert!(inspector.broker.calls.is_empty());
}

#[tokio::test]
async fn inspector_delete_queue_allows_blank_queue_delegation_like_upstream() {
    let mut inspector = inspector_with_broker(RecordingQueueLifecycleBroker::default());

    inspector.delete_queue("", true).await.unwrap();

    assert_eq!(
        inspector.broker.calls.as_slice(),
        [QueueLifecycleCall::Delete {
            queue: String::new(),
            force: true,
        }]
    );
}

#[tokio::test]
async fn inspector_task_lifecycle_methods_delegate_to_broker_like_upstream() {
    let mut inspector = inspector_with_broker(RecordingTaskLifecycleBroker::default());

    inspector.run_task("critical", "run-id").await.unwrap();
    inspector
        .archive_task("critical", "archive-id")
        .await
        .unwrap();
    inspector.delete_task("bulk", "delete-id").await.unwrap();
    inspector
        .update_task_payload("critical", "scheduled-id", b"updated".to_vec())
        .await
        .unwrap();

    assert_eq!(
        inspector.broker.calls.as_slice(),
        [
            TaskLifecycleCall::Run {
                queue: "critical".to_owned(),
                task_id: "run-id".to_owned(),
            },
            TaskLifecycleCall::Archive {
                queue: "critical".to_owned(),
                task_id: "archive-id".to_owned(),
            },
            TaskLifecycleCall::Delete {
                queue: "bulk".to_owned(),
                task_id: "delete-id".to_owned(),
            },
            TaskLifecycleCall::UpdatePayload {
                queue: "critical".to_owned(),
                task_id: "scheduled-id".to_owned(),
                payload: b"updated".to_vec(),
            },
        ]
    );
}

#[tokio::test]
async fn inspector_task_lifecycle_methods_wrap_task_not_found_like_upstream() {
    let mut inspector = inspector_with_broker(RecordingTaskLifecycleBroker {
        calls: Vec::new(),
        error: Some(AdminError::TaskNotFound),
    });

    let error = inspector.run_task("critical", "task-id").await.unwrap_err();

    assert_eq!(error, AdminError::AsynqTaskNotFound);
    assert!(error.is_task_not_found());
    assert_eq!(
        inspector.broker.calls.as_slice(),
        [TaskLifecycleCall::Run {
            queue: "critical".to_owned(),
            task_id: "task-id".to_owned(),
        }]
    );
}

#[tokio::test]
async fn inspector_task_lifecycle_methods_validate_queue_before_broker_like_upstream() {
    let mut inspector = inspector_with_broker(RecordingTaskLifecycleBroker::default());
    let expected = AdminError::AsynqInvalidQueueName;

    assert_eq!(
        inspector.run_task(" ", "task-id").await.unwrap_err(),
        expected
    );
    assert_eq!(
        inspector.archive_task("", "task-id").await.unwrap_err(),
        AdminError::AsynqArchiveQueueValidation
    );
    assert_eq!(
        inspector
            .update_task_payload("", "task-id", b"payload".to_vec())
            .await
            .unwrap_err(),
        expected
    );
    assert!(inspector.broker.calls.is_empty());
}

#[tokio::test]
async fn inspector_update_task_payload_wraps_admin_errors_like_upstream() {
    let mut inspector = inspector_with_broker(RecordingTaskLifecycleBroker {
        calls: Vec::new(),
        error: Some(AdminError::TaskNotScheduled),
    });

    let error = inspector
        .update_task_payload("critical", "task-id", b"updated".to_vec())
        .await
        .unwrap_err();

    assert_eq!(
        error,
        AdminError::Other("asynq: cannot update task that is not in scheduled state.".to_owned())
    );
    assert_eq!(
        inspector.broker.calls.as_slice(),
        [TaskLifecycleCall::UpdatePayload {
            queue: "critical".to_owned(),
            task_id: "task-id".to_owned(),
            payload: b"updated".to_vec(),
        }]
    );
}
