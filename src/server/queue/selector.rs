use rand::seq::SliceRandom;

use crate::ServerError;

use super::model::QueueConfig;
use super::normalize::normalize_queue_configs;
use super::priority::QueuePriority;
use crate::server::server_info_i32;

/// Selects the queue order used for each dequeue poll.
///
/// Reference: Asynq v0.26.0 processor `queues` method uses strict priority
/// ordering when configured, otherwise expands queue names by priority,
/// shuffles, and deduplicates to avoid starving lower-priority queues:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueSelector {
    StrictPriority(Vec<QueueConfig>),
    WeightedPriority(Vec<QueueConfig>),
}

impl QueueSelector {
    pub fn strict_priority<I, Q, R>(queues: I) -> Result<Self, ServerError>
    where
        I: IntoIterator<Item = (Q, R)>,
        Q: Into<String>,
        R: QueuePriority,
    {
        let mut queues = normalize_queue_configs(queues)?;
        queues.sort_by_key(|queue| std::cmp::Reverse(queue.priority));
        Ok(Self::StrictPriority(queues))
    }

    pub fn weighted_priority<I, Q, R>(queues: I) -> Result<Self, ServerError>
    where
        I: IntoIterator<Item = (Q, R)>,
        Q: Into<String>,
        R: QueuePriority,
    {
        Ok(Self::WeightedPriority(normalize_queue_configs(queues)?))
    }

    pub fn select(&mut self) -> Vec<String> {
        match self {
            Self::StrictPriority(queues) => queues.iter().map(|queue| queue.name.clone()).collect(),
            Self::WeightedPriority(queues) => {
                // Reference: Asynq v0.26.0 `processor.queues` skips weighted
                // expansion and shuffling when only one queue is configured:
                // <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L400-L406>.
                if queues.len() == 1 {
                    return vec![queues[0].name.clone()];
                }
                // Reference: Asynq v0.26.0 `newProcessor` normalizes queue
                // priorities by their greatest common divisor before weighted
                // processor queue selection:
                // <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L93-L99>.
                let divisor = gcd(queues.iter().map(QueueConfig::priority));
                let mut expanded = Vec::new();
                for queue in queues {
                    expanded.extend(std::iter::repeat_n(
                        queue.name.clone(),
                        queue.priority / divisor,
                    ));
                }
                expanded.shuffle(&mut rand::rng());

                let mut selected = Vec::new();
                for queue in expanded {
                    if !selected.contains(&queue) {
                        selected.push(queue);
                    }
                }
                selected
            }
        }
    }

    pub(in crate::server) fn queue_names(&self) -> Vec<String> {
        match self {
            Self::StrictPriority(queues) | Self::WeightedPriority(queues) => {
                queues.iter().map(|queue| queue.name.clone()).collect()
            }
        }
    }

    pub(in crate::server) fn queue_priorities(&self) -> Vec<(String, i32)> {
        match self {
            Self::StrictPriority(queues) | Self::WeightedPriority(queues) => queues
                .iter()
                .map(|queue| (queue.name.clone(), server_info_i32(queue.priority)))
                .collect(),
        }
    }

    pub(in crate::server) fn is_strict_priority(&self) -> bool {
        matches!(self, Self::StrictPriority(_))
    }
}

fn gcd<I>(values: I) -> usize
where
    I: IntoIterator<Item = usize>,
{
    fn pairwise_gcd(mut left: usize, mut right: usize) -> usize {
        while right > 0 {
            let remainder = left % right;
            left = right;
            right = remainder;
        }
        left
    }

    values
        .into_iter()
        .reduce(pairwise_gcd)
        .expect("queue priorities are non-empty after defaulting")
}
