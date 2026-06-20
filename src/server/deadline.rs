use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::task::TaskMessage;
use crate::task::unix_seconds;
use crate::{DEFAULT_TIMEOUT, ProcessingError};

pub(crate) fn worker_task_deadline(
    message: &TaskMessage,
    now: SystemTime,
) -> Result<Option<SystemTime>, ProcessingError> {
    Ok(match (message.timeout, message.deadline) {
        (0, 0) => {
            // Reference: Asynq v0.26.0 `processor.computeDeadline` falls back
            // to `DEFAULT_TIMEOUT` if both timeout and deadline are unset.
            // <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L523-L528>.
            Some(
                now.checked_add(DEFAULT_TIMEOUT)
                    .ok_or(ProcessingError::TimeOverflow(
                        "default task timeout deadline",
                    ))?,
            )
        }
        (timeout, 0) => Some(system_time_add_signed_seconds(
            now,
            timeout,
            "task timeout deadline",
        )?),
        (0, deadline) => Some(system_time_from_unix_seconds(deadline)),
        (timeout, deadline) => {
            // Reference: Asynq v0.26.0 `processor.computeDeadline` compares
            // `Now().Unix()+Timeout` with the message deadline when both are
            // set, so the timeout side is rounded to whole Unix seconds and
            // follows Go `int64` wrapping on overflow:
            // <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L529-L532>.
            let timeout_unix = unix_seconds(now).wrapping_add(timeout);
            Some(
                system_time_from_unix_seconds(timeout_unix)
                    .min(system_time_from_unix_seconds(deadline)),
            )
        }
    })
}

fn system_time_from_unix_seconds(seconds: i64) -> SystemTime {
    if seconds >= 0 {
        UNIX_EPOCH + Duration::from_secs(seconds as u64)
    } else {
        UNIX_EPOCH - Duration::from_secs(seconds.unsigned_abs())
    }
}

fn system_time_add_signed_seconds(
    time: SystemTime,
    seconds: i64,
    context: &'static str,
) -> Result<SystemTime, ProcessingError> {
    // Reference: Asynq v0.26.0 `processor.computeDeadline` applies nonzero
    // signed task-message timeouts with `time.Duration(msg.Timeout) *
    // time.Second`, so negative timeout values produce past deadlines:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L523-L537>.
    if seconds >= 0 {
        time.checked_add(Duration::from_secs(seconds as u64))
    } else {
        time.checked_sub(Duration::from_secs(seconds.unsigned_abs()))
    }
    .ok_or(ProcessingError::TimeOverflow(context))
}

pub(crate) fn tokio_instant_for_system_time(
    deadline: SystemTime,
    now: SystemTime,
) -> Option<tokio::time::Instant> {
    match deadline.duration_since(now) {
        Ok(duration) => tokio::time::Instant::now().checked_add(duration),
        Err(_) => Some(tokio::time::Instant::now()),
    }
}
