use super::*;
use crate::client::Clock;
use crate::server::TokioSleeper;
use crate::{EnqueueOptions, MetadataError, RedisRuntimeClient, pb};
use async_trait::async_trait;
use chrono::TimeZone;
use prost::Message;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

mod config;
mod lifecycle;
mod logging;
mod metadata;
mod registration;
mod run_once;

fn timestamp(time: SystemTime) -> prost_types::Timestamp {
    // Reference: Asynq v0.26.0 encodes scheduler metadata timestamps with
    // `timestamppb.New`, which normalizes fractional pre-epoch times to
    // non-negative nanos.
    // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L375-L390>.
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => prost_types::Timestamp {
            seconds: duration.as_secs().try_into().unwrap_or(i64::MAX),
            nanos: duration.subsec_nanos() as i32,
        },
        Err(error) => {
            let duration = error.duration();
            let seconds = i64::try_from(duration.as_secs()).unwrap_or(i64::MAX);
            let nanos = duration.subsec_nanos() as i32;
            if nanos == 0 {
                prost_types::Timestamp {
                    seconds: seconds.saturating_neg(),
                    nanos: 0,
                }
            } else {
                prost_types::Timestamp {
                    seconds: seconds.saturating_add(1).saturating_neg(),
                    nanos: 1_000_000_000 - nanos,
                }
            }
        }
    }
}

fn go_zero_time_timestamp() -> prost_types::Timestamp {
    // Reference: Asynq v0.26.0 `base.EncodeSchedulerEntry` always encodes
    // `Prev` with `timestamppb.New(entry.Prev)`. For a never-enqueued entry,
    // Go's zero `time.Time{}` is 0001-01-01T00:00:00Z.
    // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L375-L390>.
    prost_types::Timestamp {
        seconds: -62_135_596_800,
        nanos: 0,
    }
}

#[derive(Debug, Default)]
struct RecordingLogger {
    logs: Mutex<Vec<String>>,
}

impl Logger for RecordingLogger {
    fn debug(&self, args: fmt::Arguments<'_>) {
        self.logs.lock().unwrap().push(args.to_string());
    }

    fn info(&self, args: fmt::Arguments<'_>) {
        self.logs.lock().unwrap().push(args.to_string());
    }

    fn warn(&self, args: fmt::Arguments<'_>) {
        self.logs.lock().unwrap().push(args.to_string());
    }

    fn error(&self, args: fmt::Arguments<'_>) {
        self.logs.lock().unwrap().push(args.to_string());
    }

    fn fatal(&self, args: fmt::Arguments<'_>) {
        self.logs.lock().unwrap().push(args.to_string());
    }
}

#[derive(Debug, Default)]
struct RecordingSchedulerBroker {
    enqueued: Vec<EnqueuePlan>,
    enqueue_errors: VecDeque<BrokerError>,
    enqueue_error: Option<BrokerError>,
    record_event_error: Option<MetadataError>,
    write_entries_error: Option<MetadataError>,
    ping_error: Option<String>,
    ping_calls: usize,
    clear_entries_error: Option<MetadataError>,
    clear_history_error: Option<MetadataError>,
    close_calls: usize,
    metadata: Vec<SchedulerMetadataWrite>,
    events: Vec<(String, Vec<u8>, SystemTime)>,
    cleared: Vec<String>,
    cleared_history: Vec<String>,
}

type SchedulerMetadataWrite = (String, Vec<(String, Vec<u8>)>, Duration);

#[async_trait]
impl SchedulerBroker for RecordingSchedulerBroker {
    async fn ping(&mut self) -> Result<(), String> {
        self.ping_calls += 1;
        if let Some(error) = self.ping_error.clone() {
            return Err(error);
        }
        Ok(())
    }

    fn close(&mut self) {
        self.close_calls += 1;
    }

    async fn enqueue_scheduled(&mut self, plan: &EnqueuePlan) -> Result<(), BrokerError> {
        if let Some(error) = self.enqueue_errors.pop_front() {
            return Err(error);
        }
        if let Some(error) = self.enqueue_error.clone() {
            return Err(error);
        }
        self.enqueued.push(plan.clone());
        Ok(())
    }

    async fn write_scheduler_entries(
        &mut self,
        scheduler_id: &str,
        entries: Vec<(String, Vec<u8>)>,
        ttl: Duration,
    ) -> Result<(), MetadataError> {
        self.metadata.push((scheduler_id.to_owned(), entries, ttl));
        if let Some(error) = self.write_entries_error.clone() {
            return Err(error);
        }
        Ok(())
    }

    async fn record_scheduler_enqueue_event(
        &mut self,
        entry_id: &str,
        event: Vec<u8>,
        now: SystemTime,
    ) -> Result<(), MetadataError> {
        if let Some(error) = self.record_event_error.clone() {
            return Err(error);
        }
        self.events.push((entry_id.to_owned(), event, now));
        Ok(())
    }

    async fn clear_scheduler_entries(&mut self, scheduler_id: &str) -> Result<(), MetadataError> {
        self.cleared.push(scheduler_id.to_owned());
        if let Some(error) = self.clear_entries_error.clone() {
            return Err(error);
        }
        Ok(())
    }

    async fn clear_scheduler_history(&mut self, entry_id: &str) -> Result<(), MetadataError> {
        self.cleared_history.push(entry_id.to_owned());
        if let Some(error) = self.clear_history_error.clone() {
            return Err(error);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct TestClock(SystemTime);

impl Clock for TestClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

#[derive(Debug, Clone)]
struct SequenceClock {
    times: Arc<Mutex<VecDeque<SystemTime>>>,
}

impl SequenceClock {
    fn new(times: impl IntoIterator<Item = SystemTime>) -> Self {
        Self {
            times: Arc::new(Mutex::new(times.into_iter().collect())),
        }
    }
}

impl Clock for SequenceClock {
    fn now(&self) -> SystemTime {
        self.times
            .lock()
            .unwrap()
            .pop_front()
            .expect("expected recorded scheduler clock time")
    }
}

#[derive(Debug)]
struct ShutdownAfterSleeps {
    sleeps: usize,
    shutdown_after: usize,
    shutdown: watch::Sender<bool>,
}

#[async_trait]
impl Sleeper for ShutdownAfterSleeps {
    async fn sleep(&mut self, _duration: Duration) {
        self.sleeps += 1;
        if self.sleeps >= self.shutdown_after {
            let _ = self.shutdown.send(true);
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}
