use super::*;

#[test]
fn default_retry_delay_uses_upstream_wrapping_formula() {
    assert_eq!(
        DefaultRetryDelay::delay_for_retried_count_with_jitter(-2, 29),
        Duration::from_secs(2)
    );
    assert_eq!(
        DefaultRetryDelay::delay_for_retried_count_with_jitter(-1, 29),
        Duration::from_secs(16)
    );
    assert_eq!(
        DefaultRetryDelay::delay_for_retried_count_with_jitter(0, 29),
        Duration::from_secs(44)
    );
    assert_eq!(
        DefaultRetryDelay::delay_for_retried_count_with_jitter(1_000, 0),
        Duration::from_nanos(3_875_820_034_684_212_736)
    );
    assert_eq!(
        DefaultRetryDelay::delay_for_retried_count_with_jitter(2_147_483_647, 0),
        Duration::ZERO
    );
    assert_eq!(
        DefaultRetryDelay::delay_for_retried_count_with_jitter(-2_147_483_648, 0),
        Duration::from_secs(15)
    );
}

#[test]
fn retry_delay_func_adapter_calculates_delays_like_upstream() {
    let task = Task::new("email:welcome", Vec::new());
    let error = HandlerError::failed("boom");
    let mut retry_delay = RetryDelayFunc(
        |retried, observed_error: &HandlerError, observed_task: &Task| {
            assert_eq!(retried, 3);
            assert_eq!(observed_error.to_string(), "boom");
            assert_eq!(observed_task.type_name(), "email:welcome");
            Duration::from_secs(42)
        },
    );

    assert_eq!(
        retry_delay.retry_delay(3, &error, &task),
        Duration::from_secs(42)
    );
}

#[test]
fn retry_delay_method_matches_upstream_name() {
    let task = Task::new("email:welcome", Vec::new());
    let error = HandlerError::failed("boom");
    let mut retry_delay = RetryDelayFunc(
        |retried, observed_error: &HandlerError, observed_task: &Task| {
            assert_eq!(retried, 2);
            assert_eq!(observed_error.to_string(), "boom");
            assert_eq!(observed_task.type_name(), "email:welcome");
            Duration::from_secs(24)
        },
    );

    assert_eq!(
        retry_delay.retry_delay(2, &error, &task),
        Duration::from_secs(24)
    );
}

#[test]
fn retry_delay_trait_method_delegates_through_adapter_method() {
    let task = Task::new("email:welcome", Vec::new());
    let error = HandlerError::failed("boom");
    let mut retry_delay =
        RetryDelayFunc(|retried, _: &HandlerError, _: &Task| Duration::from_secs(retried as u64));

    assert_eq!(
        RetryDelay::retry_delay(&mut retry_delay, 7, &error, &task),
        Duration::from_secs(7)
    );
}

#[test]
fn is_failure_method_matches_upstream_name() {
    let error = HandlerError::skip_retry("skip");
    let mut is_failure = |observed_error: &HandlerError| {
        assert_eq!(observed_error.to_string(), "skip");
        false
    };

    assert!(!is_failure.is_failure(&error));
    assert!(DefaultIsFailure.is_failure(&HandlerError::failed("boom")));
    assert!(default_is_failure_func(&error));
}

#[test]
fn is_failure_func_adapter_matches_config_callback_shape() {
    let error = HandlerError::skip_retry("skip");
    let mut is_failure = IsFailureFunc(|observed_error: &HandlerError| {
        assert_eq!(observed_error.to_string(), "skip");
        false
    });

    assert!(!is_failure.is_failure(&error));
    assert!(!IsFailure::is_failure(&mut is_failure, &error));
}

#[test]
fn public_handler_error_sentinels_match_upstream_messages_and_helpers() {
    let skipped = HandlerError::skip_retry("skip retry for the task");
    let revoked = HandlerError::revoke_task("revoke task");

    assert_eq!(skipped.to_string(), "skip retry for the task");
    assert_eq!(revoked.to_string(), "revoke task");
    assert_eq!(
        HandlerError::LeaseExpired.to_string(),
        "asynq: task lease expired"
    );

    assert!(is_skip_retry_error(&skipped));
    assert!(is_revoke_task_error(&revoked));
    assert!(is_lease_expired_error(&HandlerError::LeaseExpired));
    assert!(!is_skip_retry_error(&HandlerError::failed("boom")));
    assert!(!is_revoke_task_error(&HandlerError::failed("boom")));
    assert!(!is_lease_expired_error(&HandlerError::failed("boom")));
}

#[test]
fn default_retry_delay_func_matches_default_retry_delay_type() {
    let task = Task::new("email:welcome", Vec::new());
    let error = HandlerError::failed("boom");
    let delay = default_retry_delay_func(1, &error, &task);

    assert!(delay >= Duration::from_secs(16));
    assert!(delay <= Duration::from_secs(74));
}
