use super::*;

#[test]
fn scheduler_entry_info_exposes_raw_enqueue_metadata() {
    let next_enqueue_at = UNIX_EPOCH + Duration::from_secs(1_700_000_060);
    let prev_enqueue_at = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let later_enqueue_at = next_enqueue_at + Duration::from_secs(60);
    let mut info = SchedulerEntryInfo::new(
        "entry-id".to_owned(),
        "@every 1m".to_owned(),
        Task::new("email:welcome", b"payload".to_vec()),
        vec![
            "Queue(\"critical\")".to_owned(),
            "MaxRetry(5)".to_owned(),
            "TaskID(\"not-parsed-by-upstream-inspector\")".to_owned(),
            "malformed".to_owned(),
        ],
        next_enqueue_at,
        Some(prev_enqueue_at),
    );

    info.id_mut().push_str("-a");
    info.id_mut().push_str("-b");
    info.spec_mut().push_str(" UTC");
    info.spec_mut().push_str(" window");
    info.task_mut().payload_mut().extend_from_slice(b"-v2");
    info.task_mut()
        .headers_mut()
        .insert("x-scheduler".to_owned(), "inspector".to_owned());
    info.enqueue_options_mut().push("Timeout(30s)".to_owned());
    info.enqueue_options_mut()
        .push("Deadline(Thu Nov 16 22:13:20 UTC 2023)".to_owned());
    *info.next_mut() = later_enqueue_at;
    *info.next_mut() = next_enqueue_at;
    *info.next_enqueue_time_mut() = later_enqueue_at;
    *info.next_enqueue_at_mut() = next_enqueue_at;
    *info.prev_mut() = None;
    *info.prev_mut() = Some(later_enqueue_at);
    *info.prev_enqueue_time_mut() = None;
    *info.prev_enqueue_at_mut() = Some(prev_enqueue_at);

    assert_eq!(info.id(), "entry-id-a-b");
    assert_eq!(info.id(), "entry-id-a-b");
    assert_eq!(info.spec(), "@every 1m UTC window");
    assert_eq!(info.spec(), "@every 1m UTC window");
    assert_eq!(info.task().task_type(), "email:welcome");
    assert_eq!(info.task().task_type(), "email:welcome");
    assert_eq!(info.task().payload(), b"payload-v2");
    assert_eq!(
        info.task().headers().get("x-scheduler").map(String::as_str),
        Some("inspector")
    );
    assert_eq!(
        info.enqueue_options(),
        &[
            "Queue(\"critical\")",
            "MaxRetry(5)",
            "TaskID(\"not-parsed-by-upstream-inspector\")",
            "malformed",
            "Timeout(30s)",
            "Deadline(Thu Nov 16 22:13:20 UTC 2023)",
        ]
    );
    assert_eq!(
        info.enqueue_options(),
        &[
            "Queue(\"critical\")",
            "MaxRetry(5)",
            "TaskID(\"not-parsed-by-upstream-inspector\")",
            "malformed",
            "Timeout(30s)",
            "Deadline(Thu Nov 16 22:13:20 UTC 2023)",
        ]
    );
    assert_eq!(info.next_enqueue_time(), next_enqueue_at);
    assert_eq!(info.next_enqueue_at(), next_enqueue_at);
    assert_eq!(info.next(), next_enqueue_at);
    assert_eq!(info.next(), next_enqueue_at);
    assert_eq!(info.prev(), Some(prev_enqueue_at));
    assert_eq!(info.prev(), Some(prev_enqueue_at));
    assert_eq!(info.prev_enqueue_time(), Some(prev_enqueue_at));
    assert_eq!(info.prev_enqueue_at(), Some(prev_enqueue_at));
}

#[test]
fn scheduler_entry_alias_matches_upstream_public_model_name() {
    let entry: SchedulerEntry = SchedulerEntryInfo::new(
        "entry-id".to_owned(),
        "@every 1m".to_owned(),
        Task::new("email:welcome", b"payload".to_vec()),
        vec!["Queue(\"critical\")".to_owned()],
        UNIX_EPOCH + Duration::from_secs(60),
        None,
    );

    assert_eq!(entry.id(), "entry-id");
    assert_eq!(entry.enqueue_options(), &["Queue(\"critical\")"]);
}
