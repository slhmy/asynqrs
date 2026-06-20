use std::collections::HashMap;

use crate::DEFAULT_QUEUE_NAME;

/// Returns the upstream default queue configuration.
///
/// Reference: Asynq v0.26.0 `defaultQueueConfig`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L409-L411>.
pub fn default_queue_config() -> HashMap<String, isize> {
    HashMap::from([(DEFAULT_QUEUE_NAME.to_owned(), 1)])
}
