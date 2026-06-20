use md5::{Digest, Md5};

use super::queue::queue_key_prefix;

pub fn unique_key(queue: &str, task_type: &str, payload: &[u8]) -> String {
    unique_key_from_optional_payload(queue, task_type, Some(payload))
}

/// Builds a uniqueness key while preserving Go's nil-vs-empty payload branch.
///
/// Reference: Asynq v0.26.0 internal `base.UniqueKey`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L199-L206>.
pub(super) fn unique_key_from_optional_payload(
    queue: &str,
    task_type: &str,
    payload: Option<&[u8]>,
) -> String {
    match payload {
        Some(payload) => {
            let checksum = Md5::digest(payload);
            format!("{}unique:{task_type}:{checksum:x}", queue_key_prefix(queue))
        }
        None => format!("{}unique:{task_type}:", queue_key_prefix(queue)),
    }
}
