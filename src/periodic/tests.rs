use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use super::*;
use crate::scheduler::SchedulerBroker;
use crate::{
    BrokerError, EnqueueOptions, EnqueuePlan, LogLevel, Logger, MetadataError, RedisRuntimeClient,
    Scheduler, SchedulerOpts, Task,
};
use async_trait::async_trait;
use tokio::sync::watch;

#[derive(Debug, Clone, Copy)]
struct TestClock(SystemTime);

impl crate::client::Clock for TestClock {
    fn now(&self) -> SystemTime {
        self.0
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

#[derive(Debug, Clone, Default)]
struct TestBroker {
    enqueued: Arc<Mutex<Vec<EnqueuePlan>>>,
}

#[async_trait]
impl SchedulerBroker for TestBroker {
    async fn ping(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn close(&mut self) {}

    async fn enqueue_scheduled(&mut self, plan: &EnqueuePlan) -> Result<(), BrokerError> {
        self.enqueued.lock().unwrap().push(plan.clone());
        Ok(())
    }

    async fn write_scheduler_entries(
        &mut self,
        _scheduler_id: &str,
        _entries: Vec<(String, Vec<u8>)>,
        _ttl: Duration,
    ) -> Result<(), MetadataError> {
        Ok(())
    }

    async fn record_scheduler_enqueue_event(
        &mut self,
        _entry_id: &str,
        _event: Vec<u8>,
        _now: SystemTime,
    ) -> Result<(), MetadataError> {
        Ok(())
    }

    async fn clear_scheduler_entries(&mut self, _scheduler_id: &str) -> Result<(), MetadataError> {
        Ok(())
    }

    async fn clear_scheduler_history(&mut self, _entry_id: &str) -> Result<(), MetadataError> {
        Ok(())
    }
}

#[derive(Debug)]
struct RecordingProvider {
    configs: VecDeque<Result<Vec<PeriodicTaskConfig>, PeriodicTaskConfigProviderError>>,
}

impl PeriodicTaskConfigProvider for RecordingProvider {
    fn get_configs(&mut self) -> Result<Vec<PeriodicTaskConfig>, PeriodicTaskConfigProviderError> {
        self.configs.pop_front().unwrap_or_else(|| Ok(Vec::new()))
    }
}

#[derive(Debug)]
struct ShutdownAfterSleeps {
    sleeps: usize,
    shutdown_after: usize,
    shutdown: watch::Sender<bool>,
}

#[async_trait]
impl crate::server::Sleeper for ShutdownAfterSleeps {
    async fn sleep(&mut self, _duration: Duration) {
        self.sleeps += 1;
        if self.sleeps >= self.shutdown_after {
            let _ = self.shutdown.send(true);
        }
        tokio::task::yield_now().await;
    }
}

mod config;
mod constructors;
mod runtime;
mod sync;
