use super::*;

pub(super) fn string_field(fields: &HashMap<String, Vec<u8>>, name: &str) -> String {
    String::from_utf8(fields.get(name).unwrap().clone()).unwrap()
}

pub(super) fn decode_msg(data: &[u8]) -> TaskMessage {
    TaskMessage::decode_from_slice(data).unwrap()
}

pub(super) fn worker_info_bytes(
    host: &str,
    pid: i32,
    server_id: &str,
    task_id: &str,
    queue: &str,
) -> Vec<u8> {
    pb::asynq::WorkerInfo {
        host: host.to_owned(),
        pid,
        server_id: server_id.to_owned(),
        task_id: task_id.to_owned(),
        task_type: "email:welcome".to_owned(),
        queue: queue.to_owned(),
        task_payload: Vec::new(),
        start_time: None,
        deadline: None,
    }
    .encode_to_vec()
}

pub(super) fn sorted(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values
}

pub(super) fn utc_date(time: SystemTime) -> String {
    let duration = time
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("Redis integration tests use post-epoch timestamps");
    let days = (duration.as_secs() / 86_400) as i64;
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
