use std::time::{SystemTime, UNIX_EPOCH};

use super::queue::queue_key_prefix;

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
