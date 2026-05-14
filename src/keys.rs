//! Redis key helpers used by Asynq.
//!
//! Reference: Asynq v0.26.0 `internal/base` Redis key helpers:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L81-L139>.

use std::time::{SystemTime, UNIX_EPOCH};

use md5::{Digest, Md5};

pub const DEFAULT_QUEUE_NAME: &str = "default";

pub const ALL_SERVERS: &str = "asynq:servers";
pub const ALL_WORKERS: &str = "asynq:workers";
pub const ALL_SCHEDULERS: &str = "asynq:schedulers";
pub const ALL_QUEUES: &str = "asynq:queues";
pub const CANCEL_CHANNEL: &str = "asynq:cancel";

pub fn default_queue() -> String {
    pending_key(DEFAULT_QUEUE_NAME)
}

pub fn queue_key_prefix(queue: &str) -> String {
    format!("asynq:{{{queue}}}:")
}

pub fn task_key_prefix(queue: &str) -> String {
    format!("{}t:", queue_key_prefix(queue))
}

pub fn task_key(queue: &str, task_id: &str) -> String {
    format!("{}{task_id}", task_key_prefix(queue))
}

pub fn pending_key(queue: &str) -> String {
    format!("{}pending", queue_key_prefix(queue))
}

pub fn active_key(queue: &str) -> String {
    format!("{}active", queue_key_prefix(queue))
}

pub fn scheduled_key(queue: &str) -> String {
    format!("{}scheduled", queue_key_prefix(queue))
}

pub fn retry_key(queue: &str) -> String {
    format!("{}retry", queue_key_prefix(queue))
}

pub fn archived_key(queue: &str) -> String {
    format!("{}archived", queue_key_prefix(queue))
}

pub fn lease_key(queue: &str) -> String {
    format!("{}lease", queue_key_prefix(queue))
}

pub fn completed_key(queue: &str) -> String {
    format!("{}completed", queue_key_prefix(queue))
}

pub fn paused_key(queue: &str) -> String {
    format!("{}paused", queue_key_prefix(queue))
}

pub fn processed_total_key(queue: &str) -> String {
    format!("{}processed", queue_key_prefix(queue))
}

pub fn processed_key(queue: &str, time: SystemTime) -> String {
    format!("{}processed:{}", queue_key_prefix(queue), utc_date(time))
}

pub fn failed_total_key(queue: &str) -> String {
    format!("{}failed", queue_key_prefix(queue))
}

pub fn failed_key(queue: &str, time: SystemTime) -> String {
    format!("{}failed:{}", queue_key_prefix(queue), utc_date(time))
}

pub fn server_info_key(hostname: &str, pid: i32, server_id: &str) -> String {
    format!("asynq:servers:{{{hostname}:{pid}:{server_id}}}")
}

pub fn workers_key(hostname: &str, pid: i32, server_id: &str) -> String {
    format!("asynq:workers:{{{hostname}:{pid}:{server_id}}}")
}

pub fn scheduler_entries_key(scheduler_id: &str) -> String {
    format!("asynq:schedulers:{{{scheduler_id}}}")
}

pub fn scheduler_history_key(entry_id: &str) -> String {
    format!("asynq:scheduler_history:{entry_id}")
}

pub fn unique_key(queue: &str, task_type: &str, payload: &[u8]) -> String {
    let checksum = Md5::digest(payload);
    format!("{}unique:{task_type}:{checksum:x}", queue_key_prefix(queue))
}

pub fn group_key_prefix(queue: &str) -> String {
    format!("{}g:", queue_key_prefix(queue))
}

pub fn group_key(queue: &str, group: &str) -> String {
    format!("{}{group}", group_key_prefix(queue))
}

pub fn aggregation_set_key(queue: &str, group: &str, set_id: &str) -> String {
    format!("{}:{set_id}", group_key(queue, group))
}

pub fn all_groups_key(queue: &str) -> String {
    format!("{}groups", queue_key_prefix(queue))
}

pub fn all_aggregation_sets_key(queue: &str) -> String {
    format!("{}aggregation_sets", queue_key_prefix(queue))
}

fn utc_date(time: SystemTime) -> String {
    let days = match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => (duration.as_secs() / 86_400) as i64,
        Err(error) => {
            let duration = error.duration();
            -((duration.as_secs() / 86_400) as i64) - i64::from(duration.as_secs() % 86_400 != 0)
        }
    };
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}")
}

fn civil_from_days(days_since_unix_epoch: i64) -> (i64, u32, u32) {
    let z = days_since_unix_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    (year, month as u32, day as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_queue_local_keys_with_hash_tags() {
        assert_eq!(queue_key_prefix("critical"), "asynq:{critical}:");
        assert_eq!(task_key_prefix("critical"), "asynq:{critical}:t:");
        assert_eq!(
            task_key("critical", "task-id"),
            "asynq:{critical}:t:task-id"
        );
        assert_eq!(pending_key("critical"), "asynq:{critical}:pending");
        assert_eq!(active_key("critical"), "asynq:{critical}:active");
        assert_eq!(scheduled_key("critical"), "asynq:{critical}:scheduled");
        assert_eq!(retry_key("critical"), "asynq:{critical}:retry");
        assert_eq!(archived_key("critical"), "asynq:{critical}:archived");
        assert_eq!(lease_key("critical"), "asynq:{critical}:lease");
        assert_eq!(completed_key("critical"), "asynq:{critical}:completed");
        assert_eq!(paused_key("critical"), "asynq:{critical}:paused");
        assert_eq!(
            processed_total_key("critical"),
            "asynq:{critical}:processed"
        );
        assert_eq!(failed_total_key("critical"), "asynq:{critical}:failed");
        assert_eq!(
            processed_key(
                "critical",
                UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000)
            ),
            "asynq:{critical}:processed:2023-11-14"
        );
        assert_eq!(
            failed_key(
                "critical",
                UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000)
            ),
            "asynq:{critical}:failed:2023-11-14"
        );
    }

    #[test]
    fn builds_global_keys() {
        assert_eq!(default_queue(), "asynq:{default}:pending");
        assert_eq!(
            server_info_key("host", 123, "server-id"),
            "asynq:servers:{host:123:server-id}"
        );
        assert_eq!(
            workers_key("host", 123, "server-id"),
            "asynq:workers:{host:123:server-id}"
        );
        assert_eq!(
            scheduler_entries_key("scheduler-id"),
            "asynq:schedulers:{scheduler-id}"
        );
        assert_eq!(
            scheduler_history_key("entry-id"),
            "asynq:scheduler_history:entry-id"
        );
    }

    #[test]
    fn builds_unique_key_from_payload_md5() {
        assert_eq!(
            unique_key("default", "email:welcome", b"payload"),
            "asynq:{default}:unique:email:welcome:321c3cf486ed509164edec1e1981fec8"
        );
        assert_eq!(
            unique_key("default", "email:welcome", b""),
            "asynq:{default}:unique:email:welcome:d41d8cd98f00b204e9800998ecf8427e"
        );
    }

    #[test]
    fn builds_group_keys() {
        assert_eq!(group_key_prefix("critical"), "asynq:{critical}:g:");
        assert_eq!(
            group_key("critical", "tenant-a"),
            "asynq:{critical}:g:tenant-a"
        );
        assert_eq!(
            aggregation_set_key("critical", "tenant-a", "set-id"),
            "asynq:{critical}:g:tenant-a:set-id"
        );
        assert_eq!(all_groups_key("critical"), "asynq:{critical}:groups");
        assert_eq!(
            all_aggregation_sets_key("critical"),
            "asynq:{critical}:aggregation_sets"
        );
    }
}
