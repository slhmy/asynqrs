use std::time::Duration;

use super::super::Server;
use super::super::defaults::{
    DEFAULT_SERVER_FORWARDER_INTERVAL, DEFAULT_SERVER_HEALTH_CHECK_INTERVAL,
    DEFAULT_SERVER_IDLE_SLEEP, DEFAULT_SERVER_JANITOR_INTERVAL, DEFAULT_SERVER_SHUTDOWN_TIMEOUT,
};
use super::super::maintenance::normalize_interval;

impl<P, S> Server<P, S> {
    pub fn with_idle_sleep(mut self, duration: Duration) -> Self {
        self.idle_sleep = normalize_interval(duration, DEFAULT_SERVER_IDLE_SLEEP);
        self
    }

    pub fn with_worker_count(mut self, worker_count: usize) -> Self {
        self.worker_count = if worker_count == 0 { 1 } else { worker_count };
        self
    }

    pub fn with_forwarder_interval(mut self, duration: Duration) -> Self {
        self.forwarder_interval = normalize_interval(duration, DEFAULT_SERVER_FORWARDER_INTERVAL);
        self
    }

    pub fn with_janitor_interval(mut self, duration: Duration) -> Self {
        self.janitor_interval = normalize_interval(duration, DEFAULT_SERVER_JANITOR_INTERVAL);
        self
    }

    pub fn with_shutdown_timeout(mut self, duration: Duration) -> Self {
        self.shutdown_timeout = normalize_interval(duration, DEFAULT_SERVER_SHUTDOWN_TIMEOUT);
        self
    }

    pub fn with_health_check_interval(mut self, duration: Duration) -> Self {
        self.health_check_interval =
            normalize_interval(duration, DEFAULT_SERVER_HEALTH_CHECK_INTERVAL);
        self
    }
}
