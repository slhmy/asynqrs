use std::time::{Duration, SystemTime};

use chrono::Utc;
use chrono_tz::Tz;
use cron::Schedule;

use crate::SchedulerError;

use super::duration::parse_duration_spec;
use super::time::system_time_to_utc;

#[derive(Debug, Clone, PartialEq)]
pub(in crate::scheduler) enum SchedulerEntrySchedule {
    Every(Duration),
    Cron {
        schedule: Box<Schedule>,
        timezone: Tz,
    },
}

impl SchedulerEntrySchedule {
    pub(in crate::scheduler) fn next_after(
        &self,
        now: SystemTime,
        spec: &str,
    ) -> Result<SystemTime, SchedulerError> {
        match self {
            Self::Every(interval) => now
                .checked_add(*interval)
                .ok_or(SchedulerError::TimeOverflow("scheduler next enqueue time")),
            Self::Cron { schedule, timezone } => {
                let now = system_time_to_utc(now)?.with_timezone(timezone);
                let next = schedule
                    .after(&now)
                    .next()
                    .ok_or_else(|| SchedulerError::NoNextEnqueueTime(spec.to_owned()))?;
                Ok(next.with_timezone(&Utc).into())
            }
        }
    }
}

pub(in crate::scheduler) fn parse_scheduler_spec(
    spec: &str,
    timezone: Tz,
) -> Result<SchedulerEntrySchedule, SchedulerError> {
    let (schedule_spec, timezone) = extract_cron_timezone(spec, timezone)?;
    if schedule_spec.starts_with("@every") {
        return parse_every_spec(schedule_spec).map(SchedulerEntrySchedule::Every);
    }
    parse_cron_spec(schedule_spec).map(|schedule| SchedulerEntrySchedule::Cron {
        schedule: Box::new(schedule),
        timezone,
    })
}

fn extract_cron_timezone(spec: &str, default_timezone: Tz) -> Result<(&str, Tz), SchedulerError> {
    // Reference: robfig/cron v3.0.1 standard parser extracts per-spec
    // `TZ=` and `CRON_TZ=` prefixes before parsing the remaining schedule:
    // <https://github.com/robfig/cron/blob/v3.0.1/parser.go#L93-L104>.
    let prefix_len = if spec.starts_with("CRON_TZ=") {
        "CRON_TZ=".len()
    } else if spec.starts_with("TZ=") {
        "TZ=".len()
    } else {
        return Ok((spec, default_timezone));
    };
    let Some(separator) = spec.find(' ') else {
        return Err(SchedulerError::UnsupportedSpec(spec.to_owned()));
    };
    let name = &spec[prefix_len..separator];
    let timezone = name
        .parse::<Tz>()
        .map_err(|_| SchedulerError::UnsupportedSpec(spec.to_owned()))?;
    Ok((spec[separator..].trim(), timezone))
}

fn parse_every_spec(spec: &str) -> Result<Duration, SchedulerError> {
    // Reference: robfig/cron v3.0.1 recognizes `@every` only through the
    // literal descriptor prefix `@every ` before parsing the duration:
    // <https://github.com/robfig/cron/blob/v3.0.1/parser.go#L451-L457>.
    let duration = spec
        .strip_prefix("@every ")
        .ok_or_else(|| SchedulerError::UnsupportedSpec(spec.to_owned()))?;
    if duration.is_empty() || duration.trim() != duration {
        return Err(SchedulerError::UnsupportedSpec(spec.to_owned()));
    }
    parse_duration_spec(duration).map_err(|()| SchedulerError::UnsupportedSpec(spec.to_owned()))
}

fn parse_cron_spec(spec: &str) -> Result<Schedule, SchedulerError> {
    // Reference: Asynq v0.26.0 `Scheduler.Register` accepts cron specs via
    // robfig/cron's standard parser, including descriptors such as `@hourly`.
    // The Rust cron crate requires a seconds field for field-based specs, but
    // upstream's default parser accepts exactly five fields, so accepted specs
    // are normalized with second zero while the original spec is kept in
    // scheduler metadata.
    if spec.starts_with('@') {
        let normalized = match spec {
            "@yearly" | "@monthly" | "@weekly" | "@daily" | "@hourly" => spec.to_owned(),
            "@annually" => "@yearly".to_owned(),
            "@midnight" => "@daily".to_owned(),
            _ => return Err(SchedulerError::UnsupportedSpec(spec.to_owned())),
        };
        return normalized
            .parse()
            .map_err(|_| SchedulerError::UnsupportedSpec(spec.to_owned()));
    }

    let trimmed = spec.trim();
    let fields = trimmed.split_whitespace().collect::<Vec<_>>();
    let normalized = match fields.len() {
        5 => {
            // Reference: robfig/cron v3.0.1 treats `?` as a wildcard in
            // standard cron fields, while the Rust cron crate is stricter in
            // some positions:
            // <https://github.com/robfig/cron/blob/v3.0.1/parser.go#L250-L252>.
            let fields = normalize_question_mark_wildcards(fields);
            format!("0 {fields}")
        }
        _ => return Err(SchedulerError::UnsupportedSpec(spec.to_owned())),
    };
    normalized
        .parse()
        .map_err(|_| SchedulerError::UnsupportedSpec(spec.to_owned()))
}

fn normalize_question_mark_wildcards(fields: Vec<&str>) -> String {
    fields
        .into_iter()
        .map(normalize_question_mark_wildcard)
        .collect::<Vec<_>>()
        .join(" ")
}

fn normalize_question_mark_wildcard(field: &str) -> String {
    // Reference: robfig/cron v3.0.1 treats `?` and `*` as full-field
    // wildcards when they appear at the start of a range expression. Suffixes
    // such as `?-5` and `*-5` keep the full wildcard range instead of using
    // the explicit high bound; a step suffix still applies to the full range:
    // <https://github.com/robfig/cron/blob/v3.0.1/parser.go#L252-L265>.
    field
        .split(',')
        .map(|expr| {
            if let Some(step) = expr.strip_prefix("?/") {
                return format!("*/{step}");
            }
            if let Some(range_suffix) = expr.strip_prefix("?-").or_else(|| expr.strip_prefix("*-"))
            {
                return range_suffix
                    .split_once('/')
                    .map(|(_, step)| format!("*/{step}"))
                    .unwrap_or_else(|| "*".to_owned());
            }
            if expr == "?" {
                "*".to_owned()
            } else {
                expr.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}
