use crate::ServerError;
use crate::task::validate_queue_name;

use super::default_queue_config;
use super::model::QueueConfig;
use super::priority::QueuePriority;

pub(super) fn normalize_queue_configs<I, Q, R>(queues: I) -> Result<Vec<QueueConfig>, ServerError>
where
    I: IntoIterator<Item = (Q, R)>,
    Q: Into<String>,
    R: QueuePriority,
{
    // Reference: Asynq v0.26.0 `NewServerFromRedisClient` ignores invalid
    // queue names and non-positive priorities in `Config.Queues`, then falls
    // back to `default_queue_config` if none remain.
    // <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L462-L477>.
    let mut queues: Vec<QueueConfig> = queues
        .into_iter()
        .filter_map(|(queue, priority)| {
            let queue = queue.into();
            let priority = priority.into_queue_priority();
            if validate_queue_name(&queue).is_err() {
                None
            } else {
                priority.map(|priority| QueueConfig::new(queue, priority))
            }
        })
        .collect();
    if queues.is_empty() {
        queues.extend(
            default_queue_config()
                .into_iter()
                .filter_map(|(queue, priority)| {
                    priority
                        .into_queue_priority()
                        .map(|priority| QueueConfig::new(queue, priority))
                }),
        );
    }
    Ok(queues)
}
