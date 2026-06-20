use std::time::Duration;

use crate::SchedulerError;
use crate::compat::{MAX_DURATION_NANOS_I128, MIN_DURATION_ABS_NANOS_I128};

pub(in crate::scheduler) fn parse_duration_spec(input: &str) -> Result<Duration, ()> {
    // Reference: Asynq v0.26.0 delegates `@every` parsing to robfig/cron,
    // which uses Go `time.ParseDuration` then `cron.Every`.
    let nanos = parse_go_duration_nanos(input)?;
    cron_every_duration(nanos)
}

pub(in crate::scheduler) fn parse_go_duration_nanos(input: &str) -> Result<i128, ()> {
    // Reference: Go `time.ParseDuration` returns an `int64` nanosecond
    // `time.Duration`, allowing one extra magnitude only for the negative
    // minimum value.
    // <https://go.dev/src/time/format.go#L1615-L1728>.
    let mut rest = input;
    let sign: i128 = if let Some(stripped) = rest.strip_prefix('-') {
        rest = stripped;
        -1
    } else {
        rest = rest.strip_prefix('+').unwrap_or(rest);
        1
    };
    if rest.is_empty() {
        return Err(());
    }
    if rest == "0" {
        return Ok(0);
    }

    let mut total_nanos = 0i128;
    while !rest.is_empty() {
        let digits_len = rest
            .char_indices()
            .take_while(|(_, ch)| ch.is_ascii_digit())
            .map(|(index, ch)| index + ch.len_utf8())
            .last()
            .unwrap_or(0);
        let whole = if digits_len == 0 {
            0
        } else {
            rest[..digits_len].parse::<i128>().map_err(|_| ())?
        };
        rest = &rest[digits_len..];
        let fraction_nanos = if let Some(stripped) = rest.strip_prefix('.') {
            let fraction_len = stripped
                .char_indices()
                .take_while(|(_, ch)| ch.is_ascii_digit())
                .map(|(index, ch)| index + ch.len_utf8())
                .last()
                .unwrap_or(0);
            if digits_len == 0 && fraction_len == 0 {
                return Err(());
            }
            let fraction = &stripped[..fraction_len];
            rest = &stripped[fraction_len..];
            let (_, multiplier) = duration_unit(rest).ok_or(())?;
            parse_go_duration_fraction_nanos(fraction, multiplier)
        } else {
            if digits_len == 0 {
                return Err(());
            }
            0
        };
        let (unit, multiplier) = duration_unit(rest).ok_or(())?;
        let whole_nanos = whole.checked_mul(multiplier).ok_or(())?;
        total_nanos = total_nanos
            .checked_add(whole_nanos.checked_add(fraction_nanos).ok_or(())?)
            .ok_or(())?;
        if total_nanos > MIN_DURATION_ABS_NANOS_I128 {
            return Err(());
        }
        rest = &rest[unit.len()..];
    }
    if sign > 0 && total_nanos > MAX_DURATION_NANOS_I128 {
        return Err(());
    }
    total_nanos.checked_mul(sign).ok_or(())
}

fn parse_go_duration_fraction_nanos(fraction: &str, multiplier: i128) -> i128 {
    // Reference: Go `time.ParseDuration` uses `leadingFraction`, which
    // consumes all fractional digits but caps accumulated precision, then
    // scales the retained prefix with float64.
    // <https://go.dev/src/time/format.go#L1574-L1602>.
    let mut value = 0_i128;
    let mut divisor = 1_f64;
    let mut overflow = false;
    for digit in fraction.bytes().map(|byte| i128::from(byte - b'0')) {
        if overflow {
            continue;
        }
        if value > MAX_DURATION_NANOS_I128 / 10 {
            overflow = true;
            continue;
        }
        let next = value * 10 + digit;
        if next > MIN_DURATION_ABS_NANOS_I128 {
            overflow = true;
            continue;
        }
        value = next;
        divisor *= 10.0;
    }
    (value as f64 * (multiplier as f64 / divisor)) as i128
}

fn cron_every_duration(nanos: i128) -> Result<Duration, ()> {
    // Reference: robfig/cron v3.0.1 `Every` rounds delays below one second up
    // to one second and truncates smaller fields:
    // <https://github.com/robfig/cron/blob/v3.0.1/constantdelay.go#L11-L20>.
    if nanos < 1_000_000_000 {
        return Ok(Duration::from_secs(1));
    }
    let seconds = nanos / 1_000_000_000;
    if seconds > u64::MAX as i128 {
        return Err(());
    }
    Ok(Duration::from_secs(seconds as u64))
}

fn duration_unit(input: &str) -> Option<(&'static str, i128)> {
    [
        ("ms", 1_000_000),
        ("µs", 1_000),
        ("μs", 1_000),
        ("us", 1_000),
        ("ns", 1),
        ("h", 3_600_000_000_000),
        ("m", 60_000_000_000),
        ("s", 1_000_000_000),
    ]
    .into_iter()
    .find(|(unit, _)| input.starts_with(unit))
}

pub(in crate::scheduler) fn format_duration_spec(
    duration: Duration,
) -> Result<String, SchedulerError> {
    if duration.is_zero() {
        return Err(SchedulerError::ZeroInterval);
    }
    if duration.subsec_nanos() == 0 {
        return Ok(format!("{}s", duration.as_secs()));
    }
    let nanos = duration.as_nanos();
    if nanos % 1_000_000 == 0 {
        return Ok(format!("{}ms", nanos / 1_000_000));
    }
    if nanos % 1_000 == 0 {
        return Ok(format!("{}us", nanos / 1_000));
    }
    Ok(format!("{nanos}ns"))
}
