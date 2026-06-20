use super::*;

#[test]
fn scheduler_defaults_match_upstream_heartbeat_interval_and_ttl() {
    let scheduler = Scheduler::with_clock(
        "scheduler-id",
        RecordingSchedulerBroker::default(),
        TestClock(SystemTime::UNIX_EPOCH),
    )
    .unwrap();

    assert_eq!(
        scheduler.heartbeat_interval,
        DEFAULT_SCHEDULER_HEARTBEAT_INTERVAL
    );
    assert_eq!(
        DEFAULT_SCHEDULER_HEARTBEAT_INTERVAL,
        DEFAULT_SCHEDULER_HEARTBEAT_INTERVAL
    );
    assert_eq!(
        scheduler.metadata_ttl,
        DEFAULT_SCHEDULER_HEARTBEAT_INTERVAL * 2
    );
    assert_eq!(scheduler.log_level(), LogLevel::Info);
    assert_eq!(scheduler.timezone, DEFAULT_SCHEDULER_TIMEZONE);
}

#[test]
fn scheduler_options_defaults_match_upstream_normalization() {
    let scheduler = Scheduler::new_with_generated_id(RecordingSchedulerBroker::default())
        .map(|scheduler| scheduler.with_scheduler_opts(SchedulerOpts::default()))
        .unwrap();

    assert_eq!(
        scheduler.heartbeat_interval,
        DEFAULT_SCHEDULER_HEARTBEAT_INTERVAL
    );
    assert_eq!(scheduler.metadata_ttl, DEFAULT_SCHEDULER_METADATA_TTL);
    assert_eq!(scheduler.log_level(), LogLevel::Info);
    assert_eq!(scheduler.timezone, DEFAULT_SCHEDULER_TIMEZONE);
}

#[test]
fn scheduler_options_treat_unspecified_log_level_as_info_like_upstream() {
    let scheduler = Scheduler::new_with_generated_id(RecordingSchedulerBroker::default())
        .map(|scheduler| {
            scheduler.with_scheduler_opts(SchedulerOpts {
                log_level: Some(LogLevel::Unspecified),
                ..SchedulerOpts::default()
            })
        })
        .unwrap();

    assert_eq!(scheduler.log_level(), LogLevel::Info);
}

#[test]
fn scheduler_opts_exposes_asynq_field_accessors() {
    let pre_hook: Arc<SchedulerEnqueueHook> = Arc::new(|_entry, _plan| {});
    let post_hook: Arc<SchedulerPostEnqueueHook> = Arc::new(|_entry, _plan, _result| {});
    let error_hook: Arc<SchedulerEnqueueErrorHook> = Arc::new(|_entry, _plan, _error| {});
    let plan_error_hook: Arc<SchedulerEnqueuePlanErrorHook> = Arc::new(|_entry, _error| {});
    let logger: Arc<dyn Logger> = Arc::new(RecordingLogger::default());
    let opts = SchedulerOpts {
        heartbeat_interval: Duration::from_secs(7),
        log_level: Some(LogLevel::Warn),
        logger: Some(Arc::clone(&logger)),
        location: Some(chrono_tz::Asia::Tokyo),
        pre_enqueue_hook: Some(Arc::clone(&pre_hook)),
        post_enqueue_hook: Some(Arc::clone(&post_hook)),
        enqueue_error_hook: Some(Arc::clone(&error_hook)),
        enqueue_plan_error_hook: Some(Arc::clone(&plan_error_hook)),
    };

    assert_eq!(opts.heartbeat_interval(), Duration::from_secs(7));
    assert_eq!(opts.log_level(), Some(LogLevel::Warn));
    assert!(Arc::ptr_eq(opts.logger().unwrap(), &logger));
    assert_eq!(opts.location(), Some(chrono_tz::Asia::Tokyo));
    assert!(Arc::ptr_eq(
        opts.enqueue_plan_error_hook().unwrap(),
        &plan_error_hook
    ));
}
