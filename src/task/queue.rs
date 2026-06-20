use thiserror::Error;

/// Default queue name for tasks without an explicit queue option.
///
/// Reference: Asynq v0.26.0 `DefaultQueueName`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L19-L24>.
pub const DEFAULT_QUEUE_NAME: &str = "default";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("queue name must contain one or more characters")]
pub struct QueueNameError;

/// Validates a queue name using Asynq's non-empty-after-trim rule.
///
/// Reference: Asynq v0.26.0 internal `base.ValidateQueueName`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L96-L103>.
pub fn validate_queue_name(name: &str) -> Result<(), QueueNameError> {
    if name.trim().is_empty() {
        return Err(QueueNameError);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_name_validation_matches_upstream() {
        assert_eq!(validate_queue_name(""), Err(QueueNameError));
        assert_eq!(validate_queue_name("   "), Err(QueueNameError));
        assert_eq!(validate_queue_name("default"), Ok(()));
        assert_eq!(validate_queue_name(" MyQueue "), Ok(()));
        assert_eq!(
            QueueNameError.to_string(),
            "queue name must contain one or more characters"
        );
    }

    #[test]
    fn default_queue_name_matches_upstream_symbol() {
        assert_eq!(DEFAULT_QUEUE_NAME, "default");
    }
}
