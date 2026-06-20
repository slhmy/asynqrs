use super::*;

#[test]
fn group_info_alias_uses_group_stats_model() {
    let mut info: GroupInfo = GroupStats::new("batch".to_owned(), 3);

    info.group_mut().push_str("-tenant");
    info.group_mut().push_str("-a");
    *info.size_mut() = 4;
    *info.size_mut() += 1;

    assert_eq!(info.group(), "batch-tenant-a");
    assert_eq!(info.group(), "batch-tenant-a");
    assert_eq!(info.size(), 5);
    assert_eq!(info.size(), 5);
}

#[test]
fn daily_stats_exposes_asynq_field_accessors() {
    let date = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let next_date = date + Duration::from_secs(86_400);
    let mut stats = DailyStats::new("critical".to_owned(), 42, 7, date);

    stats.queue_mut().push_str("-tenant");
    stats.queue_mut().push_str("-a");
    *stats.processed_mut() += 1;
    *stats.processed_mut() += 2;
    *stats.failed_mut() += 3;
    *stats.failed_mut() += 4;
    *stats.date_mut() = next_date;
    *stats.date_mut() = date;
    *stats.time_mut() = next_date;

    assert_eq!(stats.queue(), "critical-tenant-a");
    assert_eq!(stats.queue(), "critical-tenant-a");
    assert_eq!(stats.processed(), 45);
    assert_eq!(stats.processed(), 45);
    assert_eq!(stats.failed(), 14);
    assert_eq!(stats.failed(), 14);
    assert_eq!(stats.date(), next_date);
    assert_eq!(stats.date(), next_date);
    assert_eq!(stats.time(), next_date);
}

#[test]
fn queue_stats_size_includes_completed_tasks() {
    let timestamp = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let next_timestamp = timestamp + Duration::from_secs(60);
    let mut stats: QueueInfo = QueueStats::new(QueueStatsSnapshot {
        queue: "critical".to_owned(),
        memory_usage: 2048,
        paused: true,
        states: QueueStateSnapshot {
            groups: 8,
            pending: 2,
            active: 1,
            scheduled: 3,
            retry: 4,
            archived: 5,
            completed: 6,
            aggregating: 7,
        },
        throughput: QueueThroughputSnapshot {
            processed: 9,
            failed: 10,
            processed_total: 11,
            failed_total: 12,
        },
        latency: Duration::from_millis(700),
        timestamp,
    });

    stats.queue_mut().push_str("-tenant");
    stats.queue_mut().push_str("-a");
    *stats.memory_usage_mut() += 128;
    *stats.memory_usage_mut() += 256;
    *stats.paused_mut() = false;
    *stats.paused_mut() = true;
    *stats.size_mut() += 1;
    *stats.size_mut() += 2;
    *stats.groups_mut() += 3;
    *stats.groups_mut() += 4;
    *stats.pending_mut() += 5;
    *stats.pending_mut() += 6;
    *stats.active_mut() += 7;
    *stats.active_mut() += 8;
    *stats.scheduled_mut() += 9;
    *stats.scheduled_mut() += 10;
    *stats.retry_mut() += 11;
    *stats.retry_mut() += 12;
    *stats.archived_mut() += 13;
    *stats.archived_mut() += 14;
    *stats.completed_mut() += 15;
    *stats.completed_mut() += 16;
    *stats.aggregating_mut() += 17;
    *stats.aggregating_mut() += 18;
    *stats.processed_mut() += 19;
    *stats.processed_mut() += 20;
    *stats.failed_mut() += 21;
    *stats.failed_mut() += 22;
    *stats.processed_total_mut() += 23;
    *stats.processed_total_mut() += 24;
    *stats.failed_total_mut() += 25;
    *stats.failed_total_mut() += 26;
    *stats.timestamp_mut() = next_timestamp;
    *stats.timestamp_mut() = timestamp;

    assert_eq!(stats.queue(), "critical-tenant-a");
    assert_eq!(stats.queue(), "critical-tenant-a");
    assert_eq!(stats.memory_usage(), 2432);
    assert_eq!(stats.memory_usage(), 2432);
    assert!(stats.paused());
    assert!(stats.paused());
    assert_eq!(stats.groups(), 15);
    assert_eq!(stats.groups(), 15);
    assert_eq!(stats.pending(), 13);
    assert_eq!(stats.pending(), 13);
    assert_eq!(stats.active(), 16);
    assert_eq!(stats.active(), 16);
    assert_eq!(stats.scheduled(), 22);
    assert_eq!(stats.scheduled(), 22);
    assert_eq!(stats.retry(), 27);
    assert_eq!(stats.retry(), 27);
    assert_eq!(stats.archived(), 32);
    assert_eq!(stats.archived(), 32);
    assert_eq!(stats.completed(), 37);
    assert_eq!(stats.completed(), 37);
    assert_eq!(stats.aggregating(), 42);
    assert_eq!(stats.aggregating(), 42);
    assert_eq!(stats.size(), 31);
    assert_eq!(stats.size(), 31);
    assert_eq!(stats.processed(), 48);
    assert_eq!(stats.processed(), 48);
    assert_eq!(stats.failed(), 53);
    assert_eq!(stats.failed(), 53);
    assert_eq!(stats.processed_total(), 58);
    assert_eq!(stats.processed_total(), 58);
    assert_eq!(stats.failed_total(), 63);
    assert_eq!(stats.failed_total(), 63);
    assert_eq!(stats.latency(), Duration::from_millis(700));
    assert_eq!(stats.latency(), Duration::from_millis(700));
    assert_eq!(stats.latency_nanos(), 700_000_000);
    assert_eq!(stats.latency_nanos(), 700_000_000);
    stats.set_latency(Duration::from_millis(900));
    assert_eq!(stats.latency(), Duration::from_millis(900));
    assert_eq!(stats.latency_nanos(), 900_000_000);
    stats.set_latency(Duration::from_millis(700));
    assert_eq!(stats.latency(), Duration::from_millis(700));
    assert_eq!(stats.latency_nanos(), 700_000_000);
    stats.set_latency_nanos(-25_000_000_000);
    assert_eq!(stats.latency(), Duration::ZERO);
    assert_eq!(stats.latency_nanos(), -25_000_000_000);
    stats.set_latency_nanos(700_000_000);
    assert_eq!(stats.latency(), Duration::from_millis(700));
    assert_eq!(stats.latency_nanos(), 700_000_000);
    assert_eq!(stats.timestamp(), timestamp);
    assert_eq!(stats.timestamp(), timestamp);
}
